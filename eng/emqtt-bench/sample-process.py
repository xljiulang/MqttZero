#!/usr/bin/env python3
import argparse
import csv
import os
import signal
import time
from pathlib import Path


STOP_REQUESTED = False


def request_stop(_signum: int, _frame: object) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def read_status(pid: int) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = Path(f"/proc/{pid}/status").read_text(
            encoding="utf-8"
        ).splitlines()
    except (FileNotFoundError, PermissionError):
        return values

    for line in lines:
        name, separator, raw_value = line.partition(":")
        if not separator:
            continue
        parts = raw_value.strip().split()
        if not parts or not parts[0].isdigit():
            continue
        multiplier = 1024 if name.startswith("Vm") else 1
        values[name] = int(parts[0]) * multiplier
    return values


def read_smaps_rollup(pid: int) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = Path(f"/proc/{pid}/smaps_rollup").read_text(
            encoding="utf-8"
        ).splitlines()
    except (FileNotFoundError, PermissionError, OSError):
        return values

    for line in lines:
        name, separator, raw_value = line.partition(":")
        if not separator:
            continue
        parts = raw_value.strip().split()
        if not parts or not parts[0].isdigit():
            continue
        values[name] = int(parts[0]) * 1024
    return values


def read_cpu_ticks(pid: int, page_size: int) -> tuple[int, int, int, int] | None:
    try:
        content = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError):
        return None

    closing_parenthesis = content.rfind(")")
    if closing_parenthesis < 0:
        return None
    fields = content[closing_parenthesis + 2 :].split()
    if len(fields) <= 21:
        return None
    user_ticks = int(fields[11])
    system_ticks = int(fields[12])
    thread_count = int(fields[17])
    resident_bytes = int(fields[21]) * page_size
    return user_ticks, system_ticks, thread_count, resident_bytes


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval-ms", default=100, type=int)
    args = parser.parse_args()
    if args.pid <= 0:
        parser.error("--pid must be positive")
    if args.interval_ms < 10:
        parser.error("--interval-ms must be at least 10")

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    clock_ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    page_size = os.sysconf("SC_PAGE_SIZE")
    interval_seconds = args.interval_ms / 1000.0
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerow(
            [
                "TimestampNs",
                "UserTicks",
                "SystemTicks",
                "ClockTicksPerSecond",
                "ResidentBytes",
                "PeakResidentBytes",
                "PssBytes",
                "PrivateCleanBytes",
                "PrivateDirtyBytes",
                "ThreadCount",
                "VoluntaryContextSwitches",
                "NonVoluntaryContextSwitches",
            ]
        )
        target.flush()

        while not STOP_REQUESTED:
            ticks = read_cpu_ticks(args.pid, page_size)
            if ticks is None:
                break
            user_ticks, system_ticks, thread_count, resident_bytes = ticks
            status = read_status(args.pid)
            smaps = read_smaps_rollup(args.pid)
            writer.writerow(
                [
                    time.time_ns(),
                    user_ticks,
                    system_ticks,
                    clock_ticks,
                    resident_bytes,
                    status.get("VmHWM", 0),
                    smaps.get("Pss", 0),
                    smaps.get("Private_Clean", 0),
                    smaps.get("Private_Dirty", 0),
                    status.get("Threads", thread_count),
                    status.get("voluntary_ctxt_switches", 0),
                    status.get("nonvoluntary_ctxt_switches", 0),
                ]
            )
            target.flush()
            time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
