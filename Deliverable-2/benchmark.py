"""Reproducible local benchmark for static and dynamic worker scheduling."""

from __future__ import annotations

import argparse
import socket
import threading
import time
from dataclasses import dataclass

from broker import Broker
from protocol import encode_message, read_messages, validate_string


@dataclass
class RunResult:
    name: str
    workers: int
    completed: int
    elapsed: float
    recovery: float | None

    @property
    def throughput(self) -> float:
        return self.completed / self.elapsed if self.elapsed else 0.0


class BenchmarkWorker:
    def __init__(
        self, worker_id: str, host: str, port: int, delay: float, fail_first: bool = False
    ) -> None:
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.delay = delay
        self.fail_first = fail_first

    def run(self) -> None:
        with socket.create_connection((self.host, self.port), timeout=5) as conn:
            conn.settimeout(0.2)
            conn.sendall(
                encode_message({"type": "REGISTER", "worker_id": self.worker_id})
            )
            buffer = b""
            failed = False
            last_heartbeat = time.monotonic()
            while True:
                if time.monotonic() - last_heartbeat >= 0.5:
                    conn.sendall(encode_message({"type": "HEARTBEAT"}))
                    last_heartbeat = time.monotonic()
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    return
                buffer, messages = read_messages(buffer, data)
                for message in messages:
                    if message.get("type") != "TASK":
                        continue
                    if self.fail_first and not failed:
                        failed = True
                        return
                    time.sleep(self.delay)
                    task_id = validate_string(message, "task_id")
                    conn.sendall(
                        encode_message(
                            {
                                "type": "RESULT",
                                "task_id": task_id,
                                "result": "benchmark-ok",
                            }
                        )
                    )


def run_case(
    name: str, worker_count: int, task_count: int, failure: bool = False
) -> RunResult:
    broker = Broker(
        host="127.0.0.1",
        port=0,
        worker_timeout=2,
        task_timeout=2,
        max_attempts=3,
    )
    broker_thread = threading.Thread(target=broker.run, daemon=True)
    broker_thread.start()
    deadline = time.monotonic() + 3
    while broker.server is None and time.monotonic() < deadline:
        time.sleep(0.01)
    if broker.server is None:
        raise RuntimeError("broker did not start")
    port = broker.server.getsockname()[1]
    for index in range(task_count):
        broker.submit(f"benchmark-task-{index}", f"benchmark-{index}")
    workers = [
        BenchmarkWorker(
            f"worker-{index + 1}", "127.0.0.1", port, 0.04, failure and index == 0
        )
        for index in range(worker_count)
    ]
    started = time.perf_counter()
    for worker in workers:
        threading.Thread(target=worker.run, daemon=True).start()
    recovery_started = time.perf_counter() if failure else None
    while broker.completed < task_count and time.perf_counter() - started < 15:
        time.sleep(0.01)
    elapsed = time.perf_counter() - started
    recovery = (
        time.perf_counter() - recovery_started
        if failure and broker.completed == task_count and recovery_started
        else None
    )
    broker.stop()
    return RunResult(name, worker_count, broker.completed, elapsed, recovery)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=12)
    args = parser.parse_args()
    results = [
        run_case("Static baseline", 1, args.tasks),
        run_case("Dynamic load balancing", 2, args.tasks),
        run_case("Dynamic + worker failure", 2, args.tasks, failure=True),
    ]
    print("| Scenario | Workers | Completed | Elapsed (s) | Throughput (tasks/s) | Recovery (s) |")
    print("|---|---:|---:|---:|---:|---:|")
    for result in results:
        recovery = f"{result.recovery:.3f}" if result.recovery is not None else "-"
        print(
            f"| {result.name} | {result.workers} | {result.completed} | "
            f"{result.elapsed:.3f} | {result.throughput:.2f} | {recovery} |"
        )


if __name__ == "__main__":
    main()
