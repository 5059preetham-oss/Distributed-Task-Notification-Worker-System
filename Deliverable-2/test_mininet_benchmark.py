import tempfile
import unittest
from pathlib import Path

from mininet_benchmark import parse_log


class MininetBenchmarkTests(unittest.TestCase):
    def test_parser_counts_broker_results_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.log"
            path.write_text(
                "[1.000] [DISPATCH] t1 -> w1\n"
                "[1.100] [RESULT] t1 completed by worker-1: ok\n"
                "[1.200] [RESULT] t2 completed by worker-2: ok\n",
                encoding="utf-8",
            )
            result = parse_log(path, 2)
        self.assertEqual(result["completed"], 2)
        self.assertAlmostEqual(result["elapsed_seconds"], 0.2)


if __name__ == "__main__":
    unittest.main()
