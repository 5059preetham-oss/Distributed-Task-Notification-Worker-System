import time
import unittest
import socket

from broker import Broker, Task, Worker


class FakeSocket:
    def __init__(self) -> None:
        self.messages: list[bytes] = []
        self.closed = False

    def sendall(self, data: bytes) -> None:
        self.messages.append(data)

    def close(self) -> None:
        self.closed = True


class FailingSocket(FakeSocket):
    def sendall(self, data: bytes) -> None:
        raise OSError("dead")


class BrokerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.broker = Broker(max_inflight_per_worker=1, task_timeout=0.01)
        self.worker = Worker("w1", FakeSocket(), ("10.0.0.1", 1))
        self.broker.workers["w1"] = self.worker

    def test_duplicate_result_is_ignored(self) -> None:
        task = Task("t1", "payload", assigned_to="w1", assigned_at=time.monotonic())
        task.attempts = 1
        self.broker.tasks[task.task_id] = task
        self.worker.assigned.add(task.task_id)
        self.broker._handle_message(
            self.worker, {"type": "RESULT", "task_id": "t1", "result": "ok"}
        )
        self.broker._handle_message(
            self.worker, {"type": "RESULT", "task_id": "t1", "result": "ok"}
        )
        self.assertEqual(self.broker.completed, 1)

    def test_timeout_requeues_task(self) -> None:
        task = Task(
            "t1", "payload", assigned_to="w1", assigned_at=time.monotonic() - 1, attempts=1
        )
        self.broker.tasks[task.task_id] = task
        self.worker.assigned.add(task.task_id)
        self.broker._requeue_task(task, "test timeout")
        self.assertEqual(self.broker.pending.get_nowait(), "t1")

    def test_duplicate_task_id_is_rejected(self) -> None:
        self.broker.submit("first", "same-id")
        with self.assertRaises(ValueError):
            self.broker.submit("second", "same-id")

    def test_dispatch_respects_inflight_cap(self) -> None:
        self.broker.submit("a", "t1")
        self.broker.submit("b", "t2")
        self.broker._dispatch_pending(time.monotonic())
        self.assertEqual(len(self.worker.assigned), 1)
        self.assertEqual(self.broker.pending.get_nowait(), "t2")

    def test_round_robin_cycles_workers(self) -> None:
        second = Worker("w2", FakeSocket(), ("10.0.0.2", 1))
        self.broker.workers[second.worker_id] = second
        self.broker.scheduling = "round_robin"
        for index in range(2):
            task_id = self.broker.submit(f"payload-{index}", f"t{index}")
            self.broker._dispatch_pending(time.monotonic())
            assigned = self.broker.tasks[task_id].assigned_to
            self.assertEqual(assigned, f"w{index + 1}")
            worker = self.broker.workers[assigned]
            worker.assigned.clear()
            self.broker.tasks[task_id].assigned_to = None

    def test_remove_worker_identity_guard(self) -> None:
        replacement = Worker("w1", FakeSocket(), ("10.0.0.2", 1))
        self.broker.workers["w1"] = replacement
        self.broker._remove_worker("w1", "old connection", expected=self.worker)
        self.assertIs(self.broker.workers["w1"], replacement)

    def test_policy_expiry_allows_dispatch(self) -> None:
        self.worker.policy = "OVERLOADED"
        self.broker.policy_at["w1"] = time.monotonic() - 16
        task_id = self.broker.submit("payload", "t1")
        self.broker._dispatch_pending(time.monotonic())
        self.assertEqual(self.broker.tasks[task_id].assigned_to, "w1")

    def test_failed_policy_requeues_and_removes_worker(self) -> None:
        task = Task("t1", "payload", assigned_to="w1", assigned_at=time.monotonic())
        self.broker.tasks[task.task_id] = task
        self.worker.assigned.add(task.task_id)
        self.broker._handle_policy(
            {"type": "WORKER_POLICY", "worker_id": "w1", "state": "FAILED"}
        )
        self.assertNotIn("w1", self.broker.workers)
        self.assertEqual(self.broker.pending.get_nowait(), "t1")

    def test_duplicate_worker_id_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.broker._register_worker("w1", FakeSocket())

    def test_dead_worker_uses_one_attempt_in_dispatch_pass(self) -> None:
        dead = Worker("dead", FailingSocket(), ("10.0.0.2", 1))
        self.broker.workers = {"dead": dead}
        task_id = self.broker.submit("payload", "dead-task")
        self.broker._dispatch_pending(time.monotonic())
        self.assertEqual(self.broker.tasks[task_id].attempts, 1)
        self.assertNotIn("dead", self.broker.workers)

    def test_heartbeat_timeout_removes_worker(self) -> None:
        self.worker.last_seen = time.monotonic() - 10
        self.broker.worker_timeout = 1
        self.broker._remove_worker("w1", "heartbeat timeout", expected=self.worker)
        self.assertNotIn("w1", self.broker.workers)

    def test_max_attempts_exhaustion_marks_failure(self) -> None:
        self.broker.max_attempts = 1
        task = Task("t1", "payload", assigned_to="w1", attempts=1)
        self.broker.tasks["t1"] = task
        self.worker.assigned.add("t1")
        self.broker._requeue_task(task, "timeout")
        self.assertEqual(self.broker.failed, 1)

    def test_metrics_include_rtt_and_completion(self) -> None:
        self.broker.sdn_host = "127.0.0.1"
        self.worker.rtt_ewma = 0.25
        self.worker.rtt_samples = 3
        self.broker.completed_by_worker["w1"] = 4
        self.broker._metrics_queue = __import__("queue").Queue(maxsize=1)
        self.broker._send_metrics()
        metrics = self.broker._metrics_queue.get_nowait()
        entry = metrics["workers"][0]
        self.assertEqual(entry["rtt_samples"], 3)
        self.assertEqual(entry["completed"], 4)

    def test_client_receives_acceptance_and_completion(self) -> None:
        client_side, broker_side = socket.socketpair()
        thread = __import__("threading").Thread(
            target=self.broker._handle_client,
            args=(broker_side, ("local", 0)),
            daemon=True,
        )
        thread.start()
        client_side.sendall(b"SUBMIT client-payload\n")
        accepted = client_side.recv(4096).decode("utf-8")
        task_id = accepted.split()[1]
        task = self.broker.tasks[task_id]
        task.assigned_to = "w1"
        task.assigned_at = time.monotonic()
        self.worker.assigned.add(task_id)
        self.broker._handle_message(
            self.worker,
            {"type": "RESULT", "task_id": task_id, "result": "done"},
        )
        self.assertIn("TASK_DONE", client_side.recv(4096).decode("utf-8"))
        client_side.close()


if __name__ == "__main__":
    unittest.main()
