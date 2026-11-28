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
    p95: int | None = None,
    latency_valid: bool = True,
) -> dict:
    return {
        "broker": broker,
        "publishRatePerSecond": delivery_rate,
        "deliveryRatePerSecond": delivery_rate,
        "latencyP50Milliseconds": 1,
        "latencyP95Milliseconds": p95 if p95 is not None else p99,
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

    def test_throughput_cell_gates_only_throughput(self) -> None:
        summary = MODULE.summarize_cell(
            "throughput",
            1,
            500,
            65536,
            3,
            {
                "mqttzero": [
                    result("mqttzero", 3900, 11000, p95=10000)
                ] * 3,
                "mqttnet": [
                    result("mqttnet", 4300, 10000, p95=9000)
                ] * 3,
            },
        )

        self.assertFalse(summary["gatePassed"])
        self.assertFalse(summary["throughputGatePassed"])
        self.assertIsNone(summary["tailGatePassed"])
        self.assertFalse(summary["latencyGateEligible"])
        self.assertAlmostEqual(
            1.111,
            summary["p95Ratio"],
            places=3,
        )

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

    def test_latency_cell_uses_p99_for_formal_tail_gate(self) -> None:
        summary = MODULE.summarize_cell(
            "latency",
            1,
            1,
            8192,
            3,
            {
                "mqttzero": [
                    result("mqttzero", 100, 4, p95=3)
                ] * 3,
                "mqttnet": [
                    result("mqttnet", 100, 4, p95=3)
                ] * 3,
            },
        )

        self.assertTrue(summary["latencyGateEligible"])
        self.assertTrue(summary["tailGatePassed"])
        self.assertIsNone(summary["throughputGatePassed"])


if __name__ == "__main__":
    unittest.main()
