"""TCP worker that registers, processes tasks, sends heartbeats, and reports results."""

from __future__ import annotations

import argparse
import socket
import threading
import time
from typing import Any

from protocol import encode_message, read_messages, validate_string


def _log(message: str) -> None:
    print(f"[{time.time():.3f}] {message}", flush=True)


def process_task(payload: str, slowdown: float = 1.0) -> str:
    time.sleep(min(2.0, max(0.1, len(payload) / 40) * slowdown))
    return f"SUCCESS - processed '{payload}'"


class Worker:
    def __init__(
        self,
        worker_id: str,
        broker_host: str,
        broker_port: int,
        heartbeat_interval: float = 2.0,
        slowdown: float = 1.0,
    ) -> None:
        self.worker_id = worker_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.heartbeat_interval = heartbeat_interval
        self.slowdown = max(0.01, slowdown)
        self.stop_event = threading.Event()
        self.send_lock = threading.Lock()
        self.reconnect_delay = 1.0

    def send(self, conn: socket.socket, message: dict[str, Any]) -> None:
        with self.send_lock:
            conn.sendall(encode_message(message))

    def _heartbeat_loop(self, conn: socket.socket) -> None:
        while not self.stop_event.wait(self.heartbeat_interval):
            try:
                self.send(conn, {"type": "HEARTBEAT", "worker_id": self.worker_id})
            except OSError:
                return

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                _log(
                    f"[WORKER {self.worker_id}] connecting to "
                    f"{self.broker_host}:{self.broker_port}"
                )
                with socket.create_connection(
                    (self.broker_host, self.broker_port), timeout=10
                ) as conn:
                    self._run_connection(conn)
            except (ConnectionError, OSError, ValueError) as exc:
                if not self.stop_event.is_set():
                    _log(f"[WORKER] connection lost: {exc}")
                    self.stop_event.wait(self.reconnect_delay)
                    self.reconnect_delay = min(10.0, self.reconnect_delay * 2)

    def _run_connection(self, conn: socket.socket) -> None:
        conn.settimeout(1.0)
        self.send(conn, {"type": "REGISTER", "worker_id": self.worker_id})
        buffer = b""
        registered = False
        last_rx = time.monotonic()
        while not self.stop_event.is_set():
            try:
                chunk = conn.recv(4096)
            except socket.timeout:
                if time.monotonic() - last_rx > 3 * self.heartbeat_interval:
                    raise ConnectionError("broker silent")
                continue
            if not chunk:
                raise ConnectionError("broker closed the connection")
            last_rx = time.monotonic()
            buffer, messages = read_messages(buffer, chunk)
            for message in messages:
                message_type = message.get("type")
                if message_type == "REGISTERED":
                    registered = True
                    self.reconnect_delay = 1.0
                    threading.Thread(
                        target=self._heartbeat_loop, args=(conn,), daemon=True
                    ).start()
                    _log(f"[WORKER {self.worker_id}] registered")
                elif message_type == "TASK":
                    if not registered:
                        raise ValueError("received TASK before registration")
                    task_id = validate_string(message, "task_id")
                    attempt = message.get("attempt")
                    if not isinstance(attempt, int) or attempt < 1:
                        raise ValueError("TASK attempt must be a positive integer")
                    payload = validate_string(message, "payload", 4096)
                    _log(f"[TASK] {task_id}: {payload}")
                    self._process_and_report(conn, task_id, payload, attempt)
                elif message_type == "RESULT_ACK":
                    _log(f"[ACK] {message.get('task_id')}")
                elif message_type == "HEARTBEAT_ACK":
                    continue
                else:
                    _log(f"[WORKER] ignored message: {message}")

    def _process_and_report(
        self, conn: socket.socket, task_id: str, payload: str, attempt: int
    ) -> None:
        result = process_task(payload, self.slowdown)
        try:
            self.send(
                conn,
                {
                    "type": "RESULT",
                    "task_id": task_id,
                    "attempt": attempt,
                    "worker_id": self.worker_id,
                    "result": result,
                },
            )
        except OSError as exc:
            raise ConnectionError(f"result send failed: {exc}") from exc
        _log(f"[RESULT] {task_id} sent")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("worker_id", nargs="?", default="worker-1")
    parser.add_argument("broker_host", nargs="?", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--heartbeat", type=float, default=2.0)
    parser.add_argument("--slowdown", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    worker = Worker(
        args.worker_id, args.broker_host, args.port, args.heartbeat, args.slowdown
    )
    try:
        worker.run()
    except (ConnectionError, OSError) as exc:
        worker.stop_event.set()
        _log(f"[WORKER] stopped: {exc}")


if __name__ == "__main__":
    main()
