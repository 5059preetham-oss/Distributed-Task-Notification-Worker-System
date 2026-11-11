import unittest

from policy import classify


class PolicyTests(unittest.TestCase):
    def test_overload_requires_two_qualified_workers(self) -> None:
        metrics = {
            "fast": {"ip": "10.0.0.1", "rtt_ewma": 1.0, "rtt_samples": 3},
            "slow": {"ip": "10.0.0.2", "rtt_ewma": 2.0, "rtt_samples": 3},
        }
        self.assertEqual(classify(metrics, set(), 10.0, {}), {
            "fast": "HEALTHY",
            "slow": "OVERLOADED",
        })

    def test_overload_recovers_after_hold_down_and_completion(self) -> None:
        metrics = {
            "fast": {
                "ip": "10.0.0.1",
                "rtt_ewma": 1.0,
                "rtt_samples": 3,
                "completed": 2,
            },
            "slow": {
                "ip": "10.0.0.2",
                "rtt_ewma": 2.0,
                "rtt_samples": 3,
                "completed": 3,
            },
        }
        state = {
            "slow": {
                "state": "OVERLOADED",
                "changed_at": 1.0,
                "completed": 2,
            }
        }
        self.assertEqual(classify(metrics, set(), 6.0, state)["slow"], "HEALTHY")

    def test_link_down_and_missing_workers_fail(self) -> None:
        metrics = {"w1": {"ip": "10.0.0.1", "completed": 0}}
        state = {"w1": {"state": "HEALTHY"}, "w2": {"state": "HEALTHY"}}
        result = classify(metrics, {"10.0.0.1"}, 1.0, state)
        self.assertEqual(result["w1"], "FAILED")
        self.assertEqual(result["w2"], "FAILED")

    def test_reconnected_worker_recovers_from_failed(self) -> None:
        metrics = {"w1": {"ip": "10.0.0.1", "completed": 0}}
        state = {"w1": {"state": "FAILED", "changed_at": 1.0}}
        self.assertEqual(classify(metrics, set(), 2.0, state)["w1"], "HEALTHY")

    def test_removed_worker_state_does_not_reappear_forever(self) -> None:
        state = {"gone": {"state": "FAILED", "changed_at": 1.0}}
        self.assertEqual(classify({}, set(), 2.0, state), {})
        self.assertEqual(state, {})

    def test_overload_does_not_stick_forever(self) -> None:
        metrics = {
            "fast": {"ip": "10.0.0.1", "rtt_ewma": 1.0, "rtt_samples": 3},
            "slow": {"ip": "10.0.0.2", "rtt_ewma": 2.0, "rtt_samples": 3},
        }
        state: dict[str, object] = {}
        self.assertEqual(classify(metrics, set(), 0.0, state)["slow"], "OVERLOADED")
        self.assertEqual(classify(metrics, set(), 60.0, state)["slow"], "HEALTHY")
        self.assertEqual(classify(metrics, set(), 61.0, state)["slow"], "HEALTHY")

    def test_fresh_rtt_samples_can_reflag_after_release(self) -> None:
        metrics = {
            "fast": {"ip": "10.0.0.1", "rtt_ewma": 1.0, "rtt_samples": 4},
            "slow": {"ip": "10.0.0.2", "rtt_ewma": 2.0, "rtt_samples": 4},
        }
        state = {
            "slow": {
                "state": "HEALTHY",
                "changed_at": 4.0,
                "release_samples": 3,
            }
        }
        self.assertEqual(classify(metrics, set(), 5.0, state)["slow"], "OVERLOADED")

    def test_single_worker_is_never_rtt_overloaded(self) -> None:
        metrics = {
            "only": {"ip": "10.0.0.1", "rtt_ewma": 99.0, "rtt_samples": 10}
        }
        self.assertEqual(classify(metrics, set(), 1.0, {})["only"], "HEALTHY")


if __name__ == "__main__":
    unittest.main()
