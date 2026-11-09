"""TCP worker that registers, processes tasks, sends heartbeats, and reports results."""

from __future__ import annotations

import argparse
import socket
import threading
import time
from typing import Any

from protocol import encode_message, read_messages, validate_string


def process_task(payload: str) -> str:
    time.sleep(min(2.0, max(0.1, len(payload) / 40)))
    return f"SUCCESS - processed '{payload}'"


class Worker:
    def __init__(
        self,
        worker_id: str,
        broker_host: str,
        broker_port: int,
        heartbeat_interval: float = 2.0,
    ) -> None:
        self.worker_id = worker_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.heartbeat_interval = heartbeat_interval
        self.stop_event = threading.Event()
        self.send_lock = threading.Lock()

    def send(self, conn: socket.socket, message: dict[str, Any]) -> None:
        with self.send_lock:
            conn.sendall(encode_message(message))

    def run(self) -> None:
        print(
            f"[WORKER {self.worker_id}] connecting to "
            f"{self.broker_host}:{self.broker_port}",
            flush=True,
        )
        with socket.create_connection((self.broker_host, self.broker_port), timeout=10) as conn:
            conn.settimeout(1.0)
            self.send(conn, {"type": "REGISTER", "worker_id": self.worker_id})
            buffer = b""
            registered = False
            last_heartbeat = 0.0
            while not self.stop_event.is_set():
                now = time.monotonic()
                if registered and now - last_heartbeat >= self.heartbeat_interval:
                    self.send(conn, {"type": "HEARTBEAT", "worker_id": self.worker_id})
                    last_heartbeat = now
                try:
                    chunk = conn.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    raise ConnectionError("broker closed the connection")
                buffer, messages = read_messages(buffer, chunk)
                for message in messages:
                    message_type = message.get("type")
                    if message_type == "REGISTERED":
                        registered = True
                        print(f"[WORKER {self.worker_id}] registered", flush=True)
                    elif message_type == "TASK":
                        task_id = validate_string(message, "task_id")
                        payload = validate_string(message, "payload", 4096)
                        print(f"[TASK] {task_id}: {payload}", flush=True)
                        result = process_task(payload)
                        self.send(
                            conn,
                            {
                                "type": "RESULT",
                                "task_id": task_id,
                                "worker_id": self.worker_id,
                                "result": result,
                            },
                        )
                        print(f"[RESULT] {task_id} sent", flush=True)
                    elif message_type == "RESULT_ACK":
                        print(f"[ACK] {message.get('task_id')}", flush=True)
                    elif message_type == "HEARTBEAT_ACK":
                        continue
                    else:
                        print(f"[WORKER] ignored message: {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("worker_id", nargs="?", default="worker-1")
    parser.add_argument("broker_host", nargs="?", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--heartbeat", type=float, default=2.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        Worker(args.worker_id, args.broker_host, args.port, args.heartbeat).run()
    except (ConnectionError, OSError) as exc:
        print(f"[WORKER] stopped: {exc}", flush=True)


if __name__ == "__main__":
    main()
