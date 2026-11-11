"""Reproducible local benchmark for static and dynamic worker scheduling."""

from __future__ import annotations

import argparse
import csv
import matplotlib.pyplot as plt
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from broker import Broker
from protocol import encode_message, read_messages, validate_string


@dataclass
class RunResult:
    name: str
    workers: int
    tasks: int
    completed: int
    elapsed: float
    recovery: float | None
    failed: int
    worker_counts: dict[str, int]

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
        self.failed_at: float | None = None

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
                        self.failed_at = time.monotonic()
                        return
                    time.sleep(self.delay)
                    task_id = validate_string(message, "task_id")
                    conn.sendall(
                        encode_message(
                            {
                                "type": "RESULT",
                                "task_id": task_id,
                                "attempt": message.get("attempt"),
                                "result": "benchmark-ok",
                            }
                        )
                    )


def run_case(
    name: str,
    worker_count: int,
    task_count: int,
    scheduling: str,
    failure: bool = False,
    max_inflight: int = 1,
) -> RunResult:
    broker = Broker(
        host="127.0.0.1",
        port=0,
        worker_timeout=2,
        task_timeout=10,
        max_attempts=3,
        max_inflight_per_worker=max_inflight,
        scheduling=scheduling,
        client_port=0,
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
            f"worker-{index + 1}",
            "127.0.0.1",
            port,
            0.04 * (index + 1),
            failure and index == 0,
        )
        for index in range(worker_count)
    ]
    started = time.perf_counter()
    for worker in workers:
        threading.Thread(target=worker.run, daemon=True).start()
    while broker.completed < task_count and time.perf_counter() - started < 15:
        time.sleep(0.01)
    elapsed = time.perf_counter() - started
    failed_at = workers[0].failed_at if failure else None
    recovery = (
        broker.first_recovery_completed_at - failed_at
        if failed_at is not None and broker.first_recovery_completed_at is not None
        else None
    )
    worker_counts = dict(broker.completed_by_worker)
    broker.stop()
    return RunResult(
        name,
        worker_count,
        task_count,
        broker.completed,
        elapsed,
        recovery,
        broker.failed,
        worker_counts,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, nargs="+", default=[20, 50, 100])
    parser.add_argument("--workers", type=int, nargs="+", default=[2, 4, 8])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--csv", default="results/benchmark-results.csv")
    parser.add_argument("--plot", default="results/benchmark-results.png")
    args = parser.parse_args()
    Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.plot).parent.mkdir(parents=True, exist_ok=True)
    results: list[RunResult] = []
    for task_count in args.tasks:
        for worker_count in args.workers:
            for repeat in range(args.repeats):
                results.append(
                    run_case(
                        "Baseline round-robin",
                        worker_count,
                        task_count,
                        "round_robin",
                        max_inflight=worker_count,
                    )
                )
                results.append(
                    run_case(
                        "Proposed least-loaded",
                        worker_count,
                        task_count,
                        "least_loaded",
                    )
                )
                results.append(
                    run_case(
                        "Proposed worker failure",
                        worker_count,
                        task_count,
                        "least_loaded",
                        failure=True,
                    )
                )
    with Path(args.csv).open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "scenario",
                "workers",
                "tasks",
                "completed",
                "failed",
                "elapsed_seconds",
                "throughput_tasks_per_second",
                "recovery_seconds",
                "worker_counts",
            ]
        )
        for result in results:
            writer.writerow(
                [
                    result.name,
                    result.workers,
                    result.tasks,
                    result.completed,
                    result.failed,
                    f"{result.elapsed:.6f}",
                    f"{result.throughput:.6f}",
                    "" if result.recovery is None else f"{result.recovery:.6f}",
                    result.worker_counts,
                ]
            )
    print("| Scenario | Workers | Completed | Elapsed (s) | Throughput (tasks/s) | Recovery (s) | Failed |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for result in results:
        recovery = f"{result.recovery:.3f}" if result.recovery is not None else "-"
        print(
            f"| {result.name} | {result.workers} | {result.completed} | "
            f"{result.elapsed:.3f} | {result.throughput:.2f} | {recovery} | {result.failed} |"
        )
    groups = sorted({result.workers for result in results})
    for scenario in (
        "Baseline round-robin",
        "Proposed least-loaded",
        "Proposed worker failure",
    ):
        means = []
        errors = []
        for worker_count in groups:
            values = [
                result.throughput
                for result in results
                if result.name == scenario and result.workers == worker_count
            ]
            means.append(sum(values) / len(values))
            errors.append(
                (max(values) - min(values)) / 2 if len(values) > 1 else 0
            )
        plt.errorbar(groups, means, yerr=errors, marker="o", capsize=4, label=scenario)
    plt.xlabel("Workers")
    plt.ylabel("Throughput (tasks/s)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(args.plot)
    plt.close()


if __name__ == "__main__":
    main()
