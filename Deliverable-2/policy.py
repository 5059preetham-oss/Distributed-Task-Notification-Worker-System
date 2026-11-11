"""Pure worker health classification rules used by the SDN controller."""

from __future__ import annotations

from typing import Any


def classify(
    metrics: dict[str, dict[str, Any]] | list[dict[str, Any]],
    link_down_ips: set[str],
    now: float,
    state: dict[str, Any],
) -> dict[str, str]:
    """Classify workers and retain hold-down metadata in ``state``."""
    workers = (
        metrics
        if isinstance(metrics, dict)
        else {
            item["worker_id"]: item
            for item in metrics
            if item.get("worker_id") and item.get("ip")
        }
    )
    result: dict[str, str] = {}
    previous_ids = {
        worker_id
        for worker_id, prior in state.items()
        if (prior.get("state") if isinstance(prior, dict) else prior) != "FAILED"
    }
    qualified = [
        item
        for item in workers.values()
        if int(item.get("rtt_samples", 0)) >= 3
    ]
    best = min((float(item["rtt_ewma"]) for item in qualified), default=0.0)
    for worker_id, item in workers.items():
        ip = str(item.get("ip", ""))
        prior = state.get(worker_id, {})
        if isinstance(prior, str):
            prior = {"state": prior}
        prior_state = prior.get("state", "HEALTHY")
        completed = int(item.get("completed", 0))
        byte_rate = float(item.get("byte_rate_bps", 0.0))
        if ip in link_down_ips:
            current = "FAILED"
        elif prior_state == "FAILED":
            # A live metrics entry confirms that this worker has reappeared.
            current = "HEALTHY"
        elif len(qualified) >= 2 and int(item.get("rtt_samples", 0)) >= 3:
            rtt = float(item["rtt_ewma"])
            overloaded = rtt > max(1.5 * best, best + 0.3)
            if prior_state == "OVERLOADED":
                hold_down_elapsed = now - float(prior.get("changed_at", now))
                current = (
                    "HEALTHY"
                    if hold_down_elapsed >= 4
                    else "OVERLOADED"
                )
            else:
                released = int(prior.get("release_samples", -1))
                fresh_samples = (
                    released < 0
                    or int(item.get("rtt_samples", 0)) > released
                )
                current = (
                    "OVERLOADED"
                    if fresh_samples and (overloaded or byte_rate >= 100_000)
                    else "HEALTHY"
                )
        else:
            current = "OVERLOADED" if byte_rate >= 100_000 else "HEALTHY"
        result[worker_id] = current
        if worker_id not in state or prior_state != current:
            state[worker_id] = {
                "state": current,
                "changed_at": now,
                "completed": completed,
                "release_samples": (
                    int(item.get("rtt_samples", 0))
                    if current == "HEALTHY" and prior_state == "OVERLOADED"
                    else int(prior.get("release_samples", -1))
                ),
            }
        else:
            state[worker_id]["completed"] = completed
    for worker_id in previous_ids - set(workers):
        prior = state[worker_id]
        if isinstance(prior, str):
            prior = {"state": prior}
        state[worker_id] = {
            "state": "FAILED",
            "changed_at": now,
            "completed": int(prior.get("completed", 0)),
            "release_samples": -1,
        }
        result[worker_id] = "FAILED"
    for worker_id, prior in list(state.items()):
        if worker_id not in workers and worker_id not in result:
            # The controller has already reported this failed worker. Remove
            # its tombstone so a later reconnect can be classified as healthy.
            del state[worker_id]
    return result
