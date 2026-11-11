import threading
import time
import unittest

from broker import Broker
from worker import Worker


class LoopbackIntegrationTests(unittest.TestCase):
    def test_worker_failure_requeues_and_completes_all_tasks(self) -> None:
        broker = Broker(
            host="127.0.0.1",
            port=0,
            worker_timeout=1,
            task_timeout=3,
            max_attempts=5,
            client_port=0,
        )
        broker_thread = threading.Thread(target=broker.run, daemon=True)
        broker_thread.start()
        deadline = time.monotonic() + 2
        while broker.server is None and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertIsNotNone(broker.server)
        port = broker.server.getsockname()[1]
        first = Worker("worker-1", "127.0.0.1", port, heartbeat_interval=0.2, slowdown=20)
        second = Worker("worker-2", "127.0.0.1", port, heartbeat_interval=0.2)
        first_thread = threading.Thread(target=first.run, daemon=True)
        second_thread = threading.Thread(target=second.run, daemon=True)
        first_thread.start()
        second_thread.start()
        for index in range(4):
            broker.submit("integration-task-" + ("x" * 100), f"task-{index}")
        time.sleep(0.4)
        first.stop_event.set()
        deadline = time.monotonic() + 12
        while broker.completed < 4 and time.monotonic() < deadline:
            time.sleep(0.05)
        first.stop_event.set()
        second.stop_event.set()
        broker.stop()
        self.assertEqual(broker.completed, 4)
        self.assertEqual(broker.failed, 0)


if __name__ == "__main__":
    unittest.main()
