"""Reproducible Mininet benchmark for baseline and closed-loop scheduling."""

from __future__ import annotations

import argparse
import csv
import os
import re
import socket
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

DISPATCH = re.compile(r"^\[(\d+\.\d+)\] \[DISPATCH\]")
RESULT = re.compile(r"^\[(\d+\.\d+)\] \[RESULT\] \S+ completed by ")
FAILED = re.compile(r"^\[(\d+\.\d+)\] \[FAILED\]")
RECOVERY = re.compile(r"^\[(\d+\.\d+)\] \[RECOVERY-MARK\]")
STATE = re.compile(r".*\[(\d+\.\d+)\].*\[STATE\].*->(?P<state>OVERLOADED|FAILED)")


def parse_log(path: Path, tasks: int) -> dict[str, Any]:
    """Parse only broker result lines and broker dispatch timing."""
    dispatch_times: list[float] = []
    result_times: list[float] = []
    failed = 0
    recovery_times: list[float] = []
    counts: dict[str, int] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            match = DISPATCH.match(line)
            if match:
                dispatch_times.append(float(match.group(1)))
            match = RESULT.match(line)
            if match:
                result_times.append(float(match.group(1)))
                worker = line.split(" completed by ", 1)[1].split(":", 1)[0]
                counts[worker] = counts.get(worker, 0) + 1
            if FAILED.match(line):
                failed += 1
            match = RECOVERY.match(line)
            if match:
                recovery_times.append(float(match.group(1)))
    started = min(dispatch_times) if dispatch_times else None
    finished = max(result_times) if result_times else None
    elapsed = finished - started if started is not None and finished else 0.0
    return {
        "completed": len(result_times),
        "failed": failed,
        "failed_rate": failed / tasks if tasks else 0.0,
        "elapsed_seconds": elapsed,
        "throughput": len(result_times) / elapsed if elapsed else 0.0,
        "worker_counts": counts,
        # This marker is emitted after a retried task completes; it measures
        # recovery completion, not the time the recovery event began.
        "recovery_completed_at": min(recovery_times) if recovery_times else None,
    }


def first_state(path: Path, after: float | None) -> tuple[str | None, float | None]:
    if not path.exists():
        return None, None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = STATE.match(line)
        if match and (after is None or float(match.group(1)) >= after):
            return match.group("state"), float(match.group(1))
    return None, None


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"controller port {host}:{port} did not open")


def run_case(
    worker_count: int,
    tasks: int,
    arm: str,
    scenario: str,
    repeat: int,
    results_dir: Path,
) -> dict[str, Any]:
    run_id = f"{arm}-{scenario}-{worker_count}-{tasks}-{repeat}"
    broker_log = results_dir / f"{run_id}-broker.log"
    controller_log = results_dir / f"{run_id}-controller.log"
    worker_logs = [results_dir / f"{run_id}-worker-{i + 1}.log" for i in range(worker_count)]
    controller_process: subprocess.Popen[bytes] | None = None
    net: Mininet | None = None
    processes: list[Any] = []
    worker_outputs = []
    broker_output = None
    controller_output = None
    fault_at: float | None = None
    try:
        from mininet.net import Mininet
        from mininet.node import OVSKernelSwitch, RemoteController

        results_dir.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["TASK_SDN_MODE"] = "closed_loop" if arm == "closed_loop" else "baseline"
        controller_output = controller_log.open("wb")
        controller_process = subprocess.Popen(
            ["ryu-manager", "--ofp-tcp-listen-port", "6653", "controller.py"],
            cwd=Path(__file__).parent,
            env=env,
            stdout=controller_output,
            stderr=controller_output,
        )
        wait_for_port("127.0.0.1", 6653)
        net = Mininet(
            controller=RemoteController,
            switch=OVSKernelSwitch,
            autoSetMacs=True,
            autoStaticArp=True,
        )
        controller = net.addController("c0", ip="127.0.0.1", port=6653)
        switch = net.addSwitch("s1", protocols="OpenFlow13", failMode="secure")
        broker = net.addHost("broker", ip="10.0.0.10/24")
        net.addLink(broker, switch, port2=1)
        workers = []
        for index in range(worker_count):
            worker = net.addHost(f"w{index + 1}", ip=f"10.0.0.{index + 1}/24")
            net.addLink(worker, switch, port2=index + 2)
            workers.append(worker)
        net.addHost(
            "controller-host", ip="10.0.0.254/24", inNamespace=False
        )
        net.addLink(net.get("controller-host"), switch, port2=10)
        net.addHost("c1", ip="10.0.0.20/24")
        net.addLink(net.get("c1"), switch, port2=11)
        net.build()
        controller.start()
        switch.start([controller])
        broker_output = broker_log.open("wb")
        max_inflight = worker_count if arm == "baseline_rr" else 1
        schedule = "round_robin" if arm == "baseline_rr" else "least_loaded"
        processes.append(
            broker.popen(
                [
                    "python3", "broker.py", "--host", "10.0.0.10",
                    "--sdn-host", "10.0.0.254", "--demo-tasks", str(tasks),
                    "--scheduling", schedule, "--max-inflight", str(max_inflight),
                ],
                cwd=str(Path(__file__).parent),
                stdout=broker_output,
                stderr=broker_output,
            )
        )
        for index, worker in enumerate(workers):
            output = worker_logs[index].open("wb")
            worker_outputs.append(output)
            processes.append(
                worker.popen(
                    [
                        "python3", "worker.py", f"worker-{index + 1}",
                        "10.0.0.10", "--slowdown", str(1.0 + index * 0.25),
                    ],
                    cwd=str(Path(__file__).parent),
                    stdout=output,
                    stderr=output,
                )
            )
        deadline = time.monotonic() + max(30, tasks * 3)
        if scenario == "slow":
            workers[0].cmd("tc qdisc add dev w1-eth0 root netem delay 200ms")
            fault_at = time.time()
        elif scenario == "link_failure":
            # Let all workers register before interrupting traffic.
            registration_deadline = time.monotonic() + 5
            while time.monotonic() < registration_deadline:
                if all(f"[REGISTER] worker-{i + 1}" in broker_log.read_text(encoding="utf-8", errors="replace")
                       for i in range(worker_count)):
                    break
                time.sleep(0.1)
            net.configLinkStatus("s1", "w1", "down")
            fault_at = time.time()
            time.sleep(2.0)
            net.configLinkStatus("s1", "w1", "up")
        while time.monotonic() < deadline:
            parsed = parse_log(broker_log, tasks)
            if parsed["completed"] >= tasks:
                break
            time.sleep(0.25)
        parsed = parse_log(broker_log, tasks)
        detected_state, detected_at = first_state(controller_log, fault_at)
        parsed.update(
            {
                "arm": arm,
                "scenario": scenario,
                "workers": worker_count,
                "tasks": tasks,
                "detection_seconds": (
                    detected_at - fault_at if fault_at and detected_at else None
                ),
                "recovery_seconds": (
                    parsed["recovery_completed_at"] - fault_at
                    if fault_at is not None and parsed["recovery_completed_at"] is not None
                    else None
                ),
                "fault_at": fault_at,
            }
        )
        return parsed
    finally:
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                pass
        if broker_output:
            broker_output.close()
        for output in worker_outputs:
            output.close()
        if net:
            net.stop()
        if controller_process:
            controller_process.terminate()
            try:
                controller_process.wait(timeout=5)
            except (OSError, subprocess.TimeoutExpired):
                controller_process.kill()
        if controller_output:
            controller_output.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, nargs="+", default=[2, 4, 8])
    parser.add_argument("--tasks", type=int, nargs="+", default=[20, 50, 100])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--results", default="results")
    args = parser.parse_args()
    results_dir = Path(args.results)
    rows = []
    for worker_count in args.workers:
        for tasks in args.tasks:
            for repeat in range(args.repeats):
                for arm in ("baseline_rr", "baseline_least", "closed_loop"):
                    for scenario in ("normal", "slow", "link_failure"):
                        rows.append(
                            run_case(
                                worker_count, tasks, arm, scenario, repeat, results_dir
                            )
                        )
    fields = [
        "arm", "scenario", "workers", "tasks", "completed", "failed",
        "failed_rate", "elapsed_seconds", "throughput", "detection_seconds",
        "recovery_seconds", "worker_counts",
    ]
    csv_path = results_dir / "mininet-results.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fields})
    arms = ("baseline_rr", "baseline_least", "closed_loop")
    print("| Arm | Workers | Throughput mean +/- std |")
    print("|---|---:|---:|")
    for arm in arms:
        for worker_count in args.workers:
            values = [
                row["throughput"]
                for row in rows
                if row["arm"] == arm and row["workers"] == worker_count
            ]
            deviation = statistics.stdev(values) if len(values) > 1 else 0.0
            print(f"| {arm} | {worker_count} | {statistics.mean(values):.3f} +/- {deviation:.3f} |")
    figure, axes = plt.subplots(1, 3, figsize=(16, 5))
    for arm in arms:
        means = []
        errors = []
        for worker_count in args.workers:
            values = [
                row["throughput"]
                for row in rows
                if row["arm"] == arm and row["workers"] == worker_count
            ]
            means.append(statistics.mean(values))
            errors.append(statistics.stdev(values) if len(values) > 1 else 0.0)
        axes[0].errorbar(args.workers, means, yerr=errors, marker="o", label=arm)
    axes[0].set_title("Throughput by worker count")
    axes[0].set_xlabel("Workers")
    axes[0].set_ylabel("Tasks/s")
    recovery_values = []
    labels = []
    for arm in arms:
        values = [
            row["recovery_seconds"]
            for row in rows
            if row["arm"] == arm and row["recovery_seconds"] is not None
        ]
        if values:
            labels.append(arm)
            recovery_values.append(statistics.mean(values))
    axes[1].bar(labels, recovery_values)
    axes[1].set_title("Recovery by arm")
    axes[1].set_ylabel("Seconds")
    for index, arm in enumerate(arms):
        counts = []
        for worker_index in range(1, max(args.workers) + 1):
            values = [
                row["worker_counts"].get(f"worker-{worker_index}", 0)
                for row in rows
                if row["arm"] == arm
            ]
            counts.append(statistics.mean(values) if values else 0)
        axes[2].plot(range(1, len(counts) + 1), counts, marker="o", label=arm)
    axes[2].set_title("Per-worker load distribution")
    axes[2].set_xlabel("Worker index")
    axes[2].set_ylabel("Completed tasks")
    for axis in axes:
        axis.legend(fontsize="small")
    figure.tight_layout()
    figure.savefig(results_dir / "mininet-summary.png")
    plt.close(figure)


if __name__ == "__main__":
    from mininet.log import setLogLevel

    setLogLevel("info")
    main()
