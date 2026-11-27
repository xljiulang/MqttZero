import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import importlib.util


SPEC = importlib.util.spec_from_file_location(
    "summarize_matrix",
    ROOT / "summarize-matrix.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def result(
    broker: str,
    delivery_rate: float,
    p99: int,
    *,
    latency_valid: bool = True,
) -> dict:
    return {
        "broker": broker,
        "publishRatePerSecond": delivery_rate,
        "deliveryRatePerSecond": delivery_rate,
        "latencyP50Milliseconds": 1,
        "latencyP95Milliseconds": p99,
        "latencyP99Milliseconds": p99,
        "throughputValid": True,
        "latencyValid": latency_valid,
        "brokerResources": {
            "totalCpuMilliseconds": 100,
            "averagePercentOfOneCore": 50,
            "peakResidentBytes": 1024,
            "peakPssBytes": 512,
            "peakThreadCount": 4,
        },
        "clientResources": {
            "totalCpuMilliseconds": 200,
        },
    }


class MatrixSummaryTests(unittest.TestCase):
    def test_throughput_cell_uses_median_and_one_x_gate(self) -> None:
        summary = MODULE.summarize_cell(
            "throughput",
            1,
            100,
            8192,
            3,
            {
                "mqttzero": [
                    result("mqttzero", 90, 10),
                    result("mqttzero", 100, 10),
                    result("mqttzero", 110, 10),
                ],
                "mqttnet": [
                    result("mqttnet", 90, 10),
                    result("mqttnet", 100, 10),
                    result("mqttnet", 110, 10),
                ],
            },
        )

        self.assertEqual(100, summary["brokers"]["mqttzero"]["deliveryRatePerSecond"])
        self.assertEqual(1.0, summary["deliveryThroughputRatio"])
        self.assertTrue(summary["gatePassed"])

    def test_latency_cell_requires_complete_sampling(self) -> None:
        summary = MODULE.summarize_cell(
            "latency",
            1,
            100,
            8192,
            3,
            {
                "mqttzero": [
                    result("mqttzero", 100, 10, latency_valid=False),
                ],
                "mqttnet": [
                    result("mqttnet", 100, 10, latency_valid=False),
                ],
            },
        )

        self.assertFalse(summary["gatePassed"])


if __name__ == "__main__":
    unittest.main()
