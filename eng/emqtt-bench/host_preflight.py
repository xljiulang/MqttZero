#!/usr/bin/env python3
import argparse
import json
import os
import platform
import re
import subprocess
from pathlib import Path
from typing import Any


DEFAULT_EMQTT_BIN = (
    "/home/ubuntu/mqttzero-dev/tools/emqtt-bench/0.6.4/bin/emqtt_bench"
)


def parse_cpu_model(cpuinfo: str) -> str:
    match = re.search(r"^model name\s*:\s*(.+)$", cpuinfo, re.MULTILINE)
    if match is None:
        raise ValueError("Unable to find model name in /proc/cpuinfo.")
    return match.group(1).strip()


def parse_logical_cpu_count(cpuinfo: str) -> int:
    count = len(re.findall(r"^processor\s*:", cpuinfo, re.MULTILINE))
    if count <= 0:
        raise ValueError("Unable to find logical processors in /proc/cpuinfo.")
    return count


def parse_memory_bytes(meminfo: str) -> int:
    match = re.search(
        r"^MemTotal:\s*(\d+)\s*kB\s*$",
        meminfo,
        re.MULTILINE,
    )
    if match is None:
        raise ValueError("Unable to find MemTotal in /proc/meminfo.")
    return int(match.group(1)) * 1024


def validate_profile(
    expected: dict[str, Any],
    current: dict[str, Any],
) -> None:
    fields = (
        "hostId",
        "cpuModel",
        "logicalCpuCount",
        "memoryBytes",
        "os",
        "dotnetVersion",
        "emqttVersion",
        "fileDescriptorLimit",
    )
    mismatches: list[str] = []
    for field in fields:
        if expected.get(field) != current.get(field):
            mismatches.append(
                f"{field}: expected={expected.get(field)!r} "
                f"current={current.get(field)!r}"
            )

    if mismatches:
        raise ValueError(
            "Host profile does not match the approved benchmark host:\n"
            + "\n".join(mismatches)
        )


def collect_profile(host_id: str) -> dict[str, Any]:
    import resource

    cpuinfo = Path("/proc/cpuinfo").read_text(encoding="utf-8")
    meminfo = Path("/proc/meminfo").read_text(encoding="utf-8")
    emqtt_bin = os.environ.get("EMQTT_BENCH_BIN", DEFAULT_EMQTT_BIN)
    emqtt_version = parse_version(emqtt_bin)
    soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    return {
        "hostId": host_id,
        "commit": resolve_commit(),
        "cpuModel": parse_cpu_model(cpuinfo),
        "logicalCpuCount": parse_logical_cpu_count(cpuinfo),
        "memoryBytes": parse_memory_bytes(meminfo),
        "os": read_os_name(),
        "dotnetVersion": run_text(("dotnet", "--version")),
        "emqttVersion": emqtt_version,
        "fileDescriptorLimit": int(soft_limit),
        "powerMode": "n/a",
    }


def parse_version(emqtt_bin: str) -> str:
    match = re.search(r"(\d+\.\d+\.\d+)", emqtt_bin)
    if match is None:
        raise ValueError(
            "Unable to determine eMQTT-Bench version from EMQTT_BENCH_BIN."
        )
    return match.group(1)


def read_os_name() -> str:
    os_release = Path("/etc/os-release")
    if os_release.exists():
        values: dict[str, str] = {}
        for line in os_release.read_text(encoding="utf-8").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key] = value.strip().strip('"')
        name = values.get("NAME", "Linux")
        version = values.get("VERSION_ID", "")
        return f"{name} {version}".strip()
    return platform.platform()


def run_text(command: tuple[str, ...]) -> str:
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip().splitlines()[0]


def resolve_commit() -> str:
    configured = os.environ.get("MQTTZERO_BENCH_COMMIT")
    if configured:
        return configured
    try:
        return run_text(("git", "rev-parse", "HEAD"))
    except subprocess.CalledProcessError as exception:
        raise ValueError(
            "Set MQTTZERO_BENCH_COMMIT when running outside a git checkout."
        ) from exception


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host-id")
    parser.add_argument("--expected-cpus", type=int)
    parser.add_argument("--expected-min-memory-gib", type=int)
    parser.add_argument("--expected-fd-limit", type=int)
    parser.add_argument("--output")
    parser.add_argument("--validate")
    parser.add_argument("--current")
    args = parser.parse_args()

    if args.validate:
        if not args.current:
            parser.error("--validate requires --current.")
        expected = json.loads(
            Path(args.validate).read_text(encoding="utf-8")
        )
        try:
            current = collect_profile(expected["hostId"])
            write_json(Path(args.current), current)
            validate_profile(expected, current)
        except (OSError, ValueError, subprocess.CalledProcessError) as exception:
            raise SystemExit(str(exception)) from exception
        return

    required = (
        args.host_id,
        args.expected_cpus,
        args.expected_min_memory_gib,
        args.expected_fd_limit,
        args.output,
    )
    if any(value is None for value in required):
        parser.error(
            "--host-id, --expected-cpus, --expected-min-memory-gib, "
            "--expected-fd-limit and --output are required."
        )

    assert args.host_id is not None
    assert args.expected_cpus is not None
    assert args.expected_min_memory_gib is not None
    assert args.expected_fd_limit is not None
    assert args.output is not None

    try:
        profile = collect_profile(args.host_id)
    except (OSError, ValueError, subprocess.CalledProcessError) as exception:
        raise SystemExit(str(exception)) from exception

    if profile["logicalCpuCount"] != args.expected_cpus:
        raise SystemExit("Logical CPU count does not match the approved host.")
    if profile["memoryBytes"] < args.expected_min_memory_gib * 1024**3:
        raise SystemExit("Memory is below the approved host minimum.")
    if profile["fileDescriptorLimit"] != args.expected_fd_limit:
        raise SystemExit("File descriptor limit does not match the approved host.")

    write_json(Path(args.output), profile)


if __name__ == "__main__":
    main()
