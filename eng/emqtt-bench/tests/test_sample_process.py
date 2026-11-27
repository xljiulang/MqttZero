import os
import unittest
import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

SPEC = importlib.util.spec_from_file_location(
    "sample_process",
    ROOT / "sample-process.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class SampleProcessTests(unittest.TestCase):
    @unittest.skipUnless(hasattr(os, "sysconf"), "Linux /proc test")
    def test_read_cpu_ticks_returns_current_process_values(self) -> None:
        page_size = os.sysconf("SC_PAGE_SIZE")

        result = MODULE.read_cpu_ticks(os.getpid(), page_size)

        self.assertIsNotNone(result)
        user_ticks, system_ticks, thread_count, resident_bytes = result
        self.assertGreaterEqual(user_ticks, 0)
        self.assertGreaterEqual(system_ticks, 0)
        self.assertGreater(thread_count, 0)
        self.assertGreater(resident_bytes, 0)

    def test_missing_process_returns_none(self) -> None:
        result = MODULE.read_cpu_ticks(999_999_999, 4096)

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
