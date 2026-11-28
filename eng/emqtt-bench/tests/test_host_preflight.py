import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import host_preflight

from host_preflight import (
    parse_cpu_model,
    parse_logical_cpu_count,
    parse_memory_bytes,
    validate_profile,
)


class HostPreflightTests(unittest.TestCase):
    def test_parses_proc_cpu_model_and_count(self) -> None:
        text = """
processor   : 0
model name  : AMD EPYC 7K62 48-Core Processor
processor   : 1
model name  : AMD EPYC 7K62 48-Core Processor
"""
        self.assertEqual(
            "AMD EPYC 7K62 48-Core Processor",
            parse_cpu_model(text),
        )
        self.assertEqual(2, parse_logical_cpu_count(text))

    def test_parses_mem_total(self) -> None:
        self.assertEqual(
            2 * 1024 * 1024 * 1024,
            parse_memory_bytes("MemTotal:        2097152 kB\n"),
        )

    def test_rejects_mixed_hosts(self) -> None:
        with self.assertRaises(ValueError):
            validate_profile(
                {
                    "hostId": "linux-dev-2vcpu",
                    "logicalCpuCount": 2,
                },
                {
                    "hostId": "developer-laptop",
                    "logicalCpuCount": 8,
                },
            )

    def test_validate_collects_and_replaces_current_profile(self) -> None:
        expected = {
            "hostId": "linux-dev-2vcpu",
            "cpuModel": "AMD EPYC 7K62 48-Core Processor",
            "logicalCpuCount": 2,
            "memoryBytes": 3_904_782_336,
            "os": "Ubuntu 24.04",
            "dotnetVersion": "10.0.115",
            "emqttVersion": "0.6.4",
            "fileDescriptorLimit": 1_048_576,
        }
        with tempfile.TemporaryDirectory() as directory:
            approved = Path(directory) / "approved.json"
            current = Path(directory) / "current.json"
            approved.write_text(
                json.dumps(expected),
                encoding="utf-8",
            )
            arguments = [
                "host_preflight.py",
                "--validate",
                str(approved),
                "--current",
                str(current),
            ]
            with (
                patch.object(sys, "argv", arguments),
                patch.object(
                    host_preflight,
                    "collect_profile",
                    return_value=expected,
                ) as collect,
                patch.object(host_preflight, "write_json") as write,
            ):
                host_preflight.main()

            collect.assert_called_once_with("linux-dev-2vcpu")
            write.assert_called_once_with(current, expected)


if __name__ == "__main__":
    unittest.main()
