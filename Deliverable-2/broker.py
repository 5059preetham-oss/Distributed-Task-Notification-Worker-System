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
METRICS_INTERVAL = 2.0
WORKER_STATES = {"HEALTHY", "OVERLOADED", "FAILED"}


def _log(message: str) -> None:
    print(f"[{time.time():.3f}] {message}", flush=True)


@dataclass
class Task:
    task_id: str
    payload: str
    created_at: float = field(default_factory=time.monotonic)
    assigned_to: str | None = None
    assigned_at: float | None = None
    attempts: int = 0
    client: "ClientConnection | None" = None


@dataclass
class ClientConnection:
    conn: socket.socket
    send_lock: threading.Lock = field(default_factory=threading.Lock)

    def send_line(self, line: str) -> None:
        with self.send_lock:
            self.conn.sendall((line + "\n").encode("utf-8"))


@dataclass
class Worker:
    worker_id: str
    conn: socket.socket
    address: tuple[str, int]
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    assigned: set[str] = field(default_factory=set)
    last_seen: float = field(default_factory=time.monotonic)
    policy: str = "HEALTHY"
    rtt_ewma: float = 0.0
    rtt_samples: int = 0

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
        max_inflight_per_worker: int = 1,
        scheduling: str = "least_loaded",
        client_port: int = 5002,
    ) -> None:
        self.host = host
        self.port = port
        self.worker_timeout = worker_timeout
        self.task_timeout = task_timeout
        self.sdn_host = sdn_host
        self.sdn_port = sdn_port
        self.max_attempts = max_attempts
        self.max_inflight_per_worker = max(1, max_inflight_per_worker)
        if scheduling not in {"least_loaded", "round_robin"}:
            raise ValueError("scheduling must be least_loaded or round_robin")
        self.scheduling = scheduling
        self.client_port = client_port
        self._round_robin_index = 0
        self.workers: dict[str, Worker] = {}
        self.tasks: dict[str, Task] = {}
        self.pending: queue.Queue[str] = queue.Queue()
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.server: socket.socket | None = None
        self.client_server: socket.socket | None = None
        self.completed = 0
        self.failed = 0
        self.completed_by_worker: dict[str, int] = {}
        self.policy: dict[str, str] = {}
        self.policy_at: dict[str, float] = {}
        self.first_recovery_at: float | None = None
        self.first_recovery_completed_at: float | None = None
        self._metrics_thread: threading.Thread | None = None

    def submit(
        self,
        payload: str,
        task_id: str | None = None,
        client: ClientConnection | None = None,
    ) -> str:
        resolved_task_id = task_id or f"task-{uuid.uuid4().hex[:8]}"
        task = Task(resolved_task_id, payload, client=client)
        with self.lock:
            if resolved_task_id in self.tasks:
                raise ValueError(f"task ID already exists: {resolved_task_id}")
            self.tasks[task.task_id] = task
            self.pending.put(task.task_id)
        _log(f"[QUEUE] {task.task_id}: {payload}")
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
                        "rtt_ewma": worker.rtt_ewma,
                        "rtt_samples": worker.rtt_samples,
                        "completed": self.completed_by_worker.get(worker.worker_id, 0),
                    }
                    for worker in self.workers.values()
                ],
            }
        if not hasattr(self, "_metrics_queue"):
            self._metrics_queue = queue.Queue(maxsize=1)
        try:
            self._metrics_queue.put_nowait(metrics)
        except queue.Full:
            try:
                self._metrics_queue.get_nowait()
            except queue.Empty:
                pass
            self._metrics_queue.put_nowait(metrics)

    def _metrics_loop(self) -> None:
        connection: socket.socket | None = None
        buffer = b""
        while not self.stop_event.is_set():
            try:
                if connection is None:
                    connection = socket.create_connection(
                        (self.sdn_host, self.sdn_port), timeout=1.0
                    )
                    connection.settimeout(0.2)
                    _log("[SDN] metrics connection established")
                try:
                    metrics = self._metrics_queue.get(timeout=METRICS_INTERVAL)
                    connection.sendall(encode_message(metrics))
                except queue.Empty:
                    continue
                try:
                    chunk = connection.recv(4096)
                    if chunk == b"":
                        connection.close()
                        connection = None
                        buffer = b""
                        continue
                    buffer, messages = read_messages(buffer, chunk)
                    for message in messages:
                        self._handle_policy(message)
                except socket.timeout:
                    pass
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                if connection is not None:
                    _log(f"[SDN] metrics connection lost: {exc}")
                connection = None
                if not self.stop_event.wait(1.0):
                    continue
        if connection is not None:
            connection.close()

    def _handle_policy(self, message: dict[str, Any]) -> None:
        if message.get("type") != "WORKER_POLICY":
            return
        worker_id = validate_string(message, "worker_id", 64)
        state = message.get("state")
        if state not in WORKER_STATES:
            raise ValueError(f"invalid worker policy: {state}")
        with self.lock:
            self.policy[worker_id] = state
            self.policy_at[worker_id] = time.monotonic()
            worker = self.workers.get(worker_id)
            if worker and state == "FAILED":
                self._remove_worker(
                    worker_id,
                    "controller reported link failure",
                    expected=worker,
                )
            elif worker:
                worker.policy = state
        _log(f"[POLICY] {worker_id} -> {state}")

    def _notify_client(self, task: Task, line: str) -> None:
        if task.client is None:
            return
        try:
            task.client.send_line(line)
        except OSError:
            task.client = None

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
                self._dispatch_pending(now)
            self._send_metrics()

    def _dispatch_pending(self, now: float) -> None:
        available = sorted(
            self.workers.values(),
            key=lambda worker: (len(worker.assigned), worker.worker_id),
        )
        available = [
            worker
            for worker in available
            if (
                self._effective_policy(worker) == "HEALTHY"
                and len(worker.assigned) < self.max_inflight_per_worker
            )
        ]
        while available and not self.pending.empty():
            task_id = self.pending.get_nowait()
            task = self.tasks.get(task_id)
            if not task or task.assigned_to:
                continue
            if self.scheduling == "round_robin":
                worker = available[self._round_robin_index % len(available)]
                self._round_robin_index += 1
            else:
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
                _log(
                    f"[DISPATCH] {task.task_id} -> {worker.worker_id} "
                    f"(load={len(worker.assigned)})"
                )
            except OSError:
                self._requeue_task(task, "send failure")
                self._remove_worker(worker.worker_id, "send failure", expected=worker)
                if worker in available:
                    available.remove(worker)
                continue
            if worker in available and len(worker.assigned) >= self.max_inflight_per_worker:
                available.remove(worker)
            available.sort(key=lambda item: (len(item.assigned), item.worker_id))

    def _effective_policy(self, worker: Worker) -> str:
        if (
            worker.policy != "HEALTHY"
            and time.monotonic() - self.policy_at.get(worker.worker_id, 0) > 15
        ):
            worker.policy = "HEALTHY"
            self.policy[worker.worker_id] = "HEALTHY"
        return worker.policy

    def _requeue_task(self, task: Task, reason: str) -> None:
        if task.assigned_to:
            worker = self.workers.get(task.assigned_to)
            if worker:
                worker.assigned.discard(task.task_id)
        task.assigned_to = None
        task.assigned_at = None
        if task.attempts < self.max_attempts:
            self.pending.put(task.task_id)
            _log(f"[RECOVERY] {task.task_id} requeued ({reason})")
        else:
            self.failed += 1
            _log(f"[FAILED] {task.task_id}: maximum attempts exceeded")
            self._notify_client(task, f"TASK_FAILED {task.task_id}")

    def _remove_worker(
        self, worker_id: str, reason: str, expected: Worker | None = None
    ) -> None:
        current = self.workers.get(worker_id)
        if not current or (expected is not None and current is not expected):
            return
        worker = self.workers.pop(worker_id)
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
        _log(f"[WORKER] {worker_id} removed: {reason}")

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
                _log(
                    f"[RESULT] ignored late or duplicate result {task_id} "
                    f"from {worker.worker_id}"
                )
                return
            result_attempt = message.get("attempt")
            if result_attempt is not None and result_attempt != task.attempts:
                _log(f"[RESULT] ignored stale attempt {result_attempt} for {task_id}")
                return
            if task.assigned_at is not None:
                rtt = time.monotonic() - task.assigned_at
                worker.rtt_samples += 1
                worker.rtt_ewma = (
                    rtt
                    if worker.rtt_samples == 1
                    else 0.5 * worker.rtt_ewma + 0.5 * rtt
                )
            task.assigned_to = None
            task.assigned_at = None
            worker.assigned.discard(task_id)
            self.completed += 1
            if task.attempts > 1 and self.first_recovery_at is None:
                self.first_recovery_at = time.monotonic()
                _log(f"[RECOVERY-MARK] {task.task_id}")
            if task.attempts > 1 and self.first_recovery_completed_at is None:
                self.first_recovery_completed_at = time.monotonic()
            self.completed_by_worker[worker.worker_id] = (
                self.completed_by_worker.get(worker.worker_id, 0) + 1
            )
        _log(
            f"[RESULT] {task_id} completed by {worker.worker_id}: "
            f"{message.get('result', '')}"
        )
        self._notify_client(
            task,
            "TASK_DONE "
            + json.dumps(
                {
                    "task_id": task_id,
                    "result": message.get("result", ""),
                    "worker_id": worker.worker_id,
                    "attempts": task.attempts,
                },
                separators=(",", ":"),
            ),
        )
        worker.send({"type": "RESULT_ACK", "task_id": task_id})

    def _handle_client(self, conn: socket.socket, address: tuple[str, int]) -> None:
        client = ClientConnection(conn)
        try:
            with conn:
                buffer = b""
                while not self.stop_event.is_set():
                    chunk = conn.recv(4096)
                    if not chunk:
                        return
                    buffer += chunk
                    while b"\n" in buffer:
                        raw, buffer = buffer.split(b"\n", 1)
                        if len(raw) > 4096:
                            raise ValueError("client command too large")
                        line = raw.decode("utf-8").strip()
                        if not line.startswith("SUBMIT "):
                            raise ValueError("client command must be SUBMIT {payload}")
                        payload = line[7:].strip()
                        if not payload:
                            raise ValueError("SUBMIT payload must not be empty")
                        task_id = self.submit(payload, client=client)
                        client.send_line(f"ACCEPTED {task_id}")
                    if len(buffer) > 4096:
                        raise ValueError("client command too large")
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            if not self.stop_event.is_set():
                _log(f"[CLIENT] {address}: {exc}")

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
                        worker = self._register_worker(worker_id, conn, address)
                    else:
                        self._handle_message(worker, message)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            if not self.stop_event.is_set():
                _log(f"[CONNECTION] {address}: {exc}")
        finally:
            if worker:
                with self.lock:
                    self._remove_worker(
                        worker.worker_id, "disconnected", expected=worker
                    )
            else:
                conn.close()

    def _register_worker(
        self, worker_id: str, conn: socket.socket, address: tuple[str, int] = ("", 0)
    ) -> Worker:
        with self.lock:
            if worker_id in self.workers:
                raise ValueError("worker_id is already registered")
            worker = Worker(
                worker_id,
                conn,
                address,
                policy=self.policy.get(worker_id, "HEALTHY"),
            )
            self.workers[worker_id] = worker
        worker.send({"type": "REGISTERED", "worker_id": worker_id})
        _log(f"[REGISTER] {worker_id} from {address}")
        return worker

    def run(self) -> None:
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((self.host, self.port))
        self.server.listen(32)
        _log(f"[BROKER] listening on {self.host}:{self.port}")
        threading.Thread(target=self._scheduler, daemon=True).start()
        self.client_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.client_server.bind((self.host, self.client_port))
            self.client_server.listen(32)
            threading.Thread(target=self._accept_clients, daemon=True).start()
        except OSError as exc:
            _log(f"[CLIENT] listener disabled: {exc}")
            self.client_server.close()
            self.client_server = None
        if self.sdn_host:
            self._metrics_queue = queue.Queue(maxsize=1)
            self._metrics_thread = threading.Thread(
                target=self._metrics_loop, daemon=True
            )
            self._metrics_thread.start()
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
            _log("[BROKER] stopping")
        finally:
            self.stop()

    def stop(self) -> None:
        self.stop_event.set()
        if self.server:
            self.server.close()
        if self.client_server:
            self.client_server.close()
        with self.lock:
            for worker in list(self.workers.values()):
                worker.conn.close()
            self.workers.clear()
        if self._metrics_thread and self._metrics_thread.is_alive():
            self._metrics_thread.join(timeout=1.0)

    def _accept_clients(self) -> None:
        if self.client_server is None:
            return
        self.client_server.settimeout(1.0)
        while not self.stop_event.is_set():
            try:
                conn, address = self.client_server.accept()
            except OSError:
                if self.stop_event.is_set():
                    return
                continue
            threading.Thread(
                target=self._handle_client, args=(conn, address), daemon=True
            ).start()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--sdn-host", help="controller host for TCP worker metrics")
    parser.add_argument("--sdn-port", type=int, default=DEFAULT_SDN_PORT)
    parser.add_argument("--worker-timeout", type=float, default=8.0)
    parser.add_argument("--task-timeout", type=float, default=15.0)
    parser.add_argument("--demo-tasks", type=int, default=0)
    parser.add_argument("--max-inflight", type=int, default=1)
    parser.add_argument("--client-port", type=int, default=5002)
    parser.add_argument(
        "--scheduling", choices=("least_loaded", "round_robin"), default="least_loaded"
    )
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
        max_inflight_per_worker=args.max_inflight,
        scheduling=args.scheduling,
        client_port=args.client_port,
    )
    for index in range(args.demo_tasks):
        broker.submit(f"Process Data Batch {index + 1}")
    broker.run()


if __name__ == "__main__":
    main()
