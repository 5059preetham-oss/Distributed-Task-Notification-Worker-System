"""TCP task broker with scheduling, heartbeats, timeout recovery, and SDN metrics."""

from __future__ import annotations

import argparse
import json
import queue
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from protocol import encode_message, read_messages, validate_string

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 5000
DEFAULT_SDN_PORT = 5001


@dataclass
class Task:
    task_id: str
    payload: str
    created_at: float = field(default_factory=time.monotonic)
    assigned_to: str | None = None
    assigned_at: float | None = None
    attempts: int = 0


@dataclass
class Worker:
    worker_id: str
    conn: socket.socket
    address: tuple[str, int]
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    assigned: set[str] = field(default_factory=set)
    last_seen: float = field(default_factory=time.monotonic)

    def send(self, message: dict[str, Any]) -> None:
        with self.send_lock:
            self.conn.sendall(encode_message(message))


class Broker:
    """Coordinates workers over TCP and publishes worker load to an SDN controller."""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        worker_timeout: float = 8.0,
        task_timeout: float = 15.0,
        sdn_host: str | None = None,
        sdn_port: int = DEFAULT_SDN_PORT,
        max_attempts: int = 3,
    ) -> None:
        self.host = host
        self.port = port
        self.worker_timeout = worker_timeout
        self.task_timeout = task_timeout
        self.sdn_host = sdn_host
        self.sdn_port = sdn_port
        self.max_attempts = max_attempts
        self.workers: dict[str, Worker] = {}
        self.tasks: dict[str, Task] = {}
        self.pending: queue.Queue[str] = queue.Queue()
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.server: socket.socket | None = None
        self.completed = 0
        self.failed = 0

    def submit(self, payload: str, task_id: str | None = None) -> str:
        task = Task(task_id or f"task-{uuid.uuid4().hex[:8]}", payload)
        with self.lock:
            self.tasks[task.task_id] = task
            self.pending.put(task.task_id)
        print(f"[QUEUE] {task.task_id}: {payload}", flush=True)
        return task.task_id

    def _send_metrics(self) -> None:
        if not self.sdn_host:
            return
        with self.lock:
            metrics = {
                "type": "WORKER_METRICS",
                "timestamp": time.time(),
                "workers": [
                    {
                        "worker_id": worker.worker_id,
                        "ip": worker.address[0],
                        "load": len(worker.assigned),
                        "last_seen": worker.last_seen,
                    }
                    for worker in self.workers.values()
                ],
            }
        try:
            with socket.create_connection((self.sdn_host, self.sdn_port), timeout=0.5) as sock:
                sock.sendall(encode_message(metrics))
        except OSError as exc:
            print(f"[SDN] metrics unavailable: {exc}", flush=True)

    def _scheduler(self) -> None:
        while not self.stop_event.wait(0.25):
            now = time.monotonic()
            with self.lock:
                stale = [
                    worker_id
                    for worker_id, worker in self.workers.items()
                    if now - worker.last_seen > self.worker_timeout
                ]
                for worker_id in stale:
                    self._remove_worker(worker_id, "heartbeat timeout")

                for task in list(self.tasks.values()):
                    if (
                        task.assigned_to
                        and task.assigned_at
                        and now - task.assigned_at > self.task_timeout
                    ):
                        self._requeue_task(task, "task timeout")

                available = sorted(
                    self.workers.values(),
                    key=lambda worker: (len(worker.assigned), worker.worker_id),
                )
                while available and not self.pending.empty():
                    task_id = self.pending.get_nowait()
                    task = self.tasks.get(task_id)
                    if not task or task.assigned_to:
                        continue
                    worker = available[0]
                    task.assigned_to = worker.worker_id
                    task.assigned_at = now
                    task.attempts += 1
                    worker.assigned.add(task.task_id)
                    try:
                        worker.send(
                            {
                                "type": "TASK",
                                "task_id": task.task_id,
                                "payload": task.payload,
                                "attempt": task.attempts,
                            }
                        )
                        print(
                            f"[DISPATCH] {task.task_id} -> {worker.worker_id} "
                            f"(load={len(worker.assigned)})",
                            flush=True,
                        )
                    except OSError:
                        self._remove_worker(worker.worker_id, "send failure")
                    available.sort(key=lambda item: (len(item.assigned), item.worker_id))
            self._send_metrics()

    def _requeue_task(self, task: Task, reason: str) -> None:
        if task.assigned_to:
            worker = self.workers.get(task.assigned_to)
            if worker:
                worker.assigned.discard(task.task_id)
        task.assigned_to = None
        task.assigned_at = None
        if task.attempts < self.max_attempts:
            self.pending.put(task.task_id)
            print(f"[RECOVERY] {task.task_id} requeued ({reason})", flush=True)
        else:
            self.failed += 1
            print(f"[FAILED] {task.task_id}: maximum attempts exceeded", flush=True)

    def _remove_worker(self, worker_id: str, reason: str) -> None:
        worker = self.workers.pop(worker_id, None)
        if not worker:
            return
        for task_id in list(worker.assigned):
            task = self.tasks.get(task_id)
            if task:
                self._requeue_task(task, f"worker {worker_id} {reason}")
        try:
            worker.conn.close()
        except OSError:
            pass
        print(f"[WORKER] {worker_id} removed: {reason}", flush=True)

    def _handle_message(self, worker: Worker, message: dict[str, Any]) -> None:
        worker.last_seen = time.monotonic()
        message_type = message.get("type")
        if message_type == "HEARTBEAT":
            worker.send({"type": "HEARTBEAT_ACK", "timestamp": time.time()})
            return
        if message_type != "RESULT":
            raise ValueError(f"unsupported message type: {message_type}")

        task_id = validate_string(message, "task_id")
        with self.lock:
            task = self.tasks.get(task_id)
            if not task or task.assigned_to != worker.worker_id:
                raise ValueError("result does not match an assigned task")
            task.assigned_to = None
            task.assigned_at = None
            worker.assigned.discard(task_id)
            self.completed += 1
        print(
            f"[RESULT] {task_id} completed by {worker.worker_id}: "
            f"{message.get('result', '')}",
            flush=True,
        )
        worker.send({"type": "RESULT_ACK", "task_id": task_id})

    def _handle_worker(self, conn: socket.socket, address: tuple[str, int]) -> None:
        worker: Worker | None = None
        buffer = b""
        try:
            conn.settimeout(1.0)
            while not self.stop_event.is_set():
                try:
                    chunk = conn.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                buffer, messages = read_messages(buffer, chunk)
                for message in messages:
                    if worker is None:
                        if message.get("type") != "REGISTER":
                            raise ValueError("first message must be REGISTER")
                        worker_id = validate_string(message, "worker_id", 64)
                        with self.lock:
                            if worker_id in self.workers:
                                raise ValueError("worker_id is already registered")
                            worker = Worker(worker_id, conn, address)
                            self.workers[worker_id] = worker
                        worker.send({"type": "REGISTERED", "worker_id": worker_id})
                        print(f"[REGISTER] {worker_id} from {address}", flush=True)
                    else:
                        self._handle_message(worker, message)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            if not self.stop_event.is_set():
                print(f"[CONNECTION] {address}: {exc}", flush=True)
        finally:
            if worker:
                with self.lock:
                    self._remove_worker(worker.worker_id, "disconnected")
            else:
                conn.close()

    def run(self) -> None:
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(32)
        print(f"[BROKER] listening on {self.host}:{self.port}", flush=True)
        threading.Thread(target=self._scheduler, daemon=True).start()
        try:
            while not self.stop_event.is_set():
                self.server.settimeout(1.0)
                try:
                    conn, address = self.server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self.stop_event.is_set():
                        break
                    raise
                threading.Thread(
                    target=self._handle_worker, args=(conn, address), daemon=True
                ).start()
        except KeyboardInterrupt:
            print("[BROKER] stopping", flush=True)
        finally:
            self.stop()

    def stop(self) -> None:
        self.stop_event.set()
        if self.server:
            self.server.close()
        with self.lock:
            for worker in list(self.workers.values()):
                worker.conn.close()
            self.workers.clear()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--sdn-host", help="controller host for TCP worker metrics")
    parser.add_argument("--sdn-port", type=int, default=DEFAULT_SDN_PORT)
    parser.add_argument("--worker-timeout", type=float, default=8.0)
    parser.add_argument("--task-timeout", type=float, default=15.0)
    parser.add_argument("--demo-tasks", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    broker = Broker(
        args.host,
        args.port,
        args.worker_timeout,
        args.task_timeout,
        args.sdn_host,
        args.sdn_port,
    )
    for index in range(args.demo_tasks):
        broker.submit(f"Process Data Batch {index + 1}")
    broker.run()


if __name__ == "__main__":
    main()
