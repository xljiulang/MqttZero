import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import summarize


SAMPLES = [
    {
        "TimestampNs": 1_000_000_000,
        "UserTicks": 10,
        "SystemTicks": 5,
        "ClockTicksPerSecond": 100,
        "ResidentBytes": 1000,
        "PeakResidentBytes": 1500,
        "PssBytes": 800,
        "PrivateCleanBytes": 100,
        "PrivateDirtyBytes": 700,
        "ThreadCount": 4,
        "VoluntaryContextSwitches": 2,
        "NonVoluntaryContextSwitches": 1,
    },
    {
        "TimestampNs": 2_000_000_000,
        "UserTicks": 20,
        "SystemTicks": 10,
        "ClockTicksPerSecond": 100,
        "ResidentBytes": 2000,
        "PeakResidentBytes": 2500,
        "PssBytes": 900,
        "PrivateCleanBytes": 200,
        "PrivateDirtyBytes": 800,
        "ThreadCount": 5,
        "VoluntaryContextSwitches": 5,
        "NonVoluntaryContextSwitches": 2,
    },
]


class SummarizeTests(unittest.TestCase):
    def test_load_benchmark_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "benchmark-config.json"
            path.write_text(
                json.dumps(
                    {
                        "broker": "mqttzero",
                        "maxBufferedBytes": 134217728,
                    }
                ),
                encoding="utf-8",
            )
            configuration = summarize.load_benchmark_configuration(
                str(path)
            )

        self.assertEqual("mqttzero", configuration["broker"])
        self.assertEqual(
            134217728,
            configuration["maxBufferedBytes"],
        )

    def test_missing_benchmark_configuration_returns_none(self) -> None:
        self.assertIsNone(
            summarize.load_benchmark_configuration(None)
        )

    def test_qoe_loader_filters_warmup_and_sorts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "qoe.csv"
            path.write_text(
                "ClientId,TS,TCP,Handshake,Connect,Subscribe,Publish\n"
                "1,99,,,,,1\n"
                "1,100,,,,,7\n"
                "2,101,,,,,3\n",
                encoding="utf-8",
            )

            result = summarize.load_qoe_latencies(str(path), 100)

        self.assertEqual([3, 7], result)

    def test_process_summary_converts_cpu_ticks_and_peaks(self) -> None:
        result = summarize.summarize_process(
            SAMPLES,
            1_000_000_000,
            2_000_000_000,
        )

        self.assertIsNotNone(result)
        self.assertEqual(150.0, result["totalCpuMilliseconds"])
        self.assertEqual(15.0, result["averagePercentOfOneCore"])
        self.assertEqual(2500, result["peakResidentBytes"])
        self.assertEqual(900, result["peakPssBytes"])
        self.assertEqual(5, result["peakThreadCount"])

    def test_missing_samples_return_none(self) -> None:
        self.assertIsNone(summarize.summarize_process([], 1, 2))

    def test_mqtt_zero_memory_probe_finds_numeric_peaks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broker.log"
            path.write_text(
                "READY\n"
                "MEMORYPROBE "
                + json.dumps(
                    {
                        "TimestampTicks": 1,
                        "ManagedHeapBytes": 10,
                        "EgressDataBatchCount": 3,
                        "ObjectPools": {"Ignored": 100},
                    }
                )
                + "\n"
                "MEMORYPROBE "
                + json.dumps(
                    {
                        "TimestampTicks": 2,
                        "ManagedHeapBytes": 20,
                        "EgressDataBatchCount": 5,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            result = summarize.load_mqtt_zero_memory(str(path))

        self.assertIsNotNone(result)
        self.assertEqual(2, result["sampleCount"])
        self.assertEqual(20, result["peak"]["ManagedHeapBytes"])
        self.assertEqual(5, result["peak"]["EgressDataBatchCount"])
        self.assertNotIn("TimestampTicks", result["peak"])


if __name__ == "__main__":
    unittest.main()
