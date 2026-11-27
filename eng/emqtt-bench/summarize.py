#!/usr/bin/env python3
import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any


def percentile(values: list[int], ratio: float) -> int | None:
    if not values:
        return None
    index = max(0, math.ceil(len(values) * ratio) - 1)
    return values[index]


def load_qoe_latencies(path: str, started_ms: int) -> list[int]:
    latencies: list[int] = []
    with Path(path).open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            publish_latency = row["Publish"]
            if not publish_latency:
                continue
            if int(row["TS"]) < started_ms:
                continue
            latencies.append(int(publish_latency))
    latencies.sort()
    return latencies


def load_samples(path: str | None) -> list[dict[str, int]]:
    if not path:
        return []
    sample_path = Path(path)
    if not sample_path.exists() or sample_path.stat().st_size == 0:
        return []

    samples: list[dict[str, int]] = []
    with sample_path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            samples.append({key: int(value) for key, value in row.items()})
    return samples


def closest_sample(
    samples: list[dict[str, int]],
    timestamp_ns: int,
    *,
    prefer_after: bool,
) -> dict[str, int] | None:
    if not samples:
        return None
    if prefer_after:
        for sample in samples:
            if sample["TimestampNs"] >= timestamp_ns:
                return sample
        return samples[-1]

    for sample in reversed(samples):
        if sample["TimestampNs"] <= timestamp_ns:
            return sample
    return samples[0]


def summarize_process(
    samples: list[dict[str, int]],
    start_ns: int,
    end_ns: int,
) -> dict[str, Any] | None:
    if not samples:
        return None
    start_sample = closest_sample(samples, start_ns, prefer_after=True)
    end_sample = closest_sample(samples, end_ns, prefer_after=False)
    if start_sample is None or end_sample is None:
        return None

    clock_ticks = max(1, start_sample["ClockTicksPerSecond"])
    user_start = start_sample["UserTicks"]
    system_start = start_sample["SystemTicks"]
    user_end = end_sample["UserTicks"]
    system_end = end_sample["SystemTicks"]
    user_ms = max(0, user_end - user_start) * 1000.0 / clock_ticks
    system_ms = max(0, system_end - system_start) * 1000.0 / clock_ticks
    total_ms = user_ms + system_ms
    wall_ms = max(1, end_sample["TimestampNs"] - start_sample["TimestampNs"]) / 1e6
    start_resident_bytes = next(
        (
            sample["ResidentBytes"]
            for sample in samples
            if sample["ResidentBytes"] > 0
        ),
        0,
    )
    end_resident_bytes = next(
        (
            sample["ResidentBytes"]
            for sample in reversed(samples)
            if sample["ResidentBytes"] > 0
        ),
        0,
    )

    return {
        "sampleCount": len(samples),
        "userCpuMilliseconds": user_ms,
        "systemCpuMilliseconds": system_ms,
        "totalCpuMilliseconds": total_ms,
        "averagePercentOfOneCore": total_ms / wall_ms * 100.0,
        "startResidentBytes": start_resident_bytes,
        "endResidentBytes": end_resident_bytes,
        "peakResidentBytes": max(
            sample["PeakResidentBytes"] or sample["ResidentBytes"]
            for sample in samples
        ),
        "peakPssBytes": max(sample["PssBytes"] for sample in samples),
        "peakPrivateDirtyBytes": max(
            sample["PrivateDirtyBytes"] for sample in samples
        ),
        "peakThreadCount": max(sample["ThreadCount"] for sample in samples),
        "voluntaryContextSwitches": max(
            0,
            samples[-1]["VoluntaryContextSwitches"]
            - samples[0]["VoluntaryContextSwitches"],
        ),
        "nonVoluntaryContextSwitches": max(
            0,
            samples[-1]["NonVoluntaryContextSwitches"]
            - samples[0]["NonVoluntaryContextSwitches"],
        ),
    }


def combine_client_resources(
    subscriber: dict[str, Any] | None,
    publisher: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if subscriber is None and publisher is None:
        return None
    resources = [resource for resource in (subscriber, publisher) if resource]
    return {
        "subscriber": subscriber,
        "publisher": publisher,
        "totalCpuMilliseconds": sum(
            resource["totalCpuMilliseconds"] for resource in resources
        ),
        "startResidentBytes": sum(
            resource["startResidentBytes"] for resource in resources
        ),
        "endResidentBytes": sum(
            resource["endResidentBytes"] for resource in resources
        ),
        "peakResidentBytes": sum(
            resource["peakResidentBytes"] for resource in resources
        ),
        "peakPssBytes": sum(resource["peakPssBytes"] for resource in resources),
        "peakPrivateDirtyBytes": sum(
            resource["peakPrivateDirtyBytes"] for resource in resources
        ),
        "peakThreadCount": sum(
            resource["peakThreadCount"] for resource in resources
        ),
    }


def load_mqtt_zero_memory(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    log_path = Path(path)
    if not log_path.exists():
        return None

    peaks: dict[str, int] = {}
    sample_count = 0
    for line in log_path.read_text(encoding="utf-8").splitlines():
        prefix = "MEMORYPROBE "
        if not line.startswith(prefix):
            continue
        try:
            snapshot = json.loads(line[len(prefix) :])
        except json.JSONDecodeError:
            continue
        if not isinstance(snapshot, dict):
            continue
        sample_count += 1
        for key, value in snapshot.items():
            if key == "TimestampTicks" or not isinstance(value, int):
                continue
            peaks[key] = max(peaks.get(key, 0), value)

    if sample_count == 0:
        return None
    return {"sampleCount": sample_count, "peak": peaks}


def add_rate_metrics(
    broker_resources: dict[str, Any] | None,
    messages: int,
    received: int,
) -> None:
    if broker_resources is None:
        return
    cpu_ms = broker_resources["totalCpuMilliseconds"]
    broker_resources["cpuMillisecondsPer1000PublishedMessages"] = (
        cpu_ms / messages * 1000.0 if messages else 0.0
    )
    broker_resources["cpuMillisecondsPer1000Deliveries"] = (
        cpu_ms / received * 1000.0 if received else 0.0
    )


def write_markdown(path: str, result: dict[str, Any]) -> None:
    broker = result.get("brokerResources") or {}
    client = result.get("clientResources") or {}
    lines = [
        f"# eMQTT-Bench {result['broker']} result",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| QoS | {result['qos']} |",
        f"| Subscribers | {result['subscribers']} |",
        f"| Payload bytes | {result['payloadBytes']} |",
        f"| Published messages | {result['publishedMessages']} |",
        f"| Received deliveries | {result['receivedDeliveries']} |",
        f"| Publish rate | {result['publishRatePerSecond']:.2f} msg/s |",
        f"| Delivery rate | {result['deliveryRatePerSecond']:.2f} delivery/s |",
        f"| p50 | {result['latencyP50Milliseconds']} ms |",
        f"| p95 | {result['latencyP95Milliseconds']} ms |",
        f"| p99 | {result['latencyP99Milliseconds']} ms |",
        f"| Latency coverage | {result['latencyCoverage']:.2%} |",
        f"| Throughput valid | {result['throughputValid']} |",
        f"| Latency valid | {result['latencyValid']} |",
        f"| Broker CPU | {broker.get('totalCpuMilliseconds', 0):.1f} ms |",
        f"| Broker CPU one core | {broker.get('averagePercentOfOneCore', 0):.1f}% |",
        f"| Broker peak RSS | {broker.get('peakResidentBytes', 0)} bytes |",
        f"| Broker peak PSS | {broker.get('peakPssBytes', 0)} bytes |",
        f"| Client CPU | {client.get('totalCpuMilliseconds', 0):.1f} ms |",
        f"| Client peak RSS | {client.get('peakResidentBytes', 0)} bytes |",
    ]
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown-output", required=True)
    parser.add_argument("--broker", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--qos", required=True, type=int)
    parser.add_argument("--subscribers", required=True, type=int)
    parser.add_argument("--payload", required=True, type=int)
    parser.add_argument("--messages", required=True, type=int)
    parser.add_argument("--warmup-messages", required=True, type=int)
    parser.add_argument("--publish-interval-ms", required=True, type=int)
    parser.add_argument("--started-ms", required=True, type=int)
    parser.add_argument("--elapsed-seconds", required=True, type=float)
    parser.add_argument(
        "--delivery-elapsed-seconds",
        required=True,
        type=float,
    )
    parser.add_argument("--received-before", required=True, type=int)
    parser.add_argument("--received-after", required=True, type=int)
    parser.add_argument("--expected-deliveries", required=True, type=int)
    parser.add_argument("--phases", required=True)
    parser.add_argument("--broker-samples", required=True)
    parser.add_argument("--subscriber-samples", required=True)
    parser.add_argument("--publisher-samples", required=True)
    parser.add_argument("--broker-log", required=True)
    args = parser.parse_args()

    latencies = load_qoe_latencies(args.csv, args.started_ms)
    received = args.received_after - args.received_before
    coverage = len(latencies) / received if received else 0.0
    complete = received == args.expected_deliveries
    latency_complete = coverage >= 0.99
    throughput_valid = complete
    latency_valid = complete and latency_complete
    phases = json.loads(Path(args.phases).read_text(encoding="utf-8"))

    broker_resources = summarize_process(
        load_samples(args.broker_samples),
        phases["MeasurementStartNs"],
        phases["MeasurementDrainedNs"],
    )
    add_rate_metrics(broker_resources, args.messages, received)
    subscriber_resources = summarize_process(
        load_samples(args.subscriber_samples),
        phases["MeasurementStartNs"],
        phases["MeasurementDrainedNs"],
    )
    publisher_resources = summarize_process(
        load_samples(args.publisher_samples),
        phases["MeasurementStartNs"],
        phases["PublishCompleteNs"],
    )

    result = {
        "broker": args.broker,
        "runId": args.run_id,
        "qos": args.qos,
        "subscribers": args.subscribers,
        "payloadBytes": args.payload,
        "warmupMessages": args.warmup_messages,
        "publishIntervalMilliseconds": args.publish_interval_ms,
        "publishedMessages": args.messages,
        "expectedDeliveries": args.expected_deliveries,
        "receivedDeliveries": received,
        "complete": complete,
        "latencyComplete": latency_complete,
        "throughputValid": throughput_valid,
        "latencyValid": latency_valid,
        "valid": throughput_valid and latency_valid,
        "publishElapsedSeconds": args.elapsed_seconds,
        "deliveryElapsedSeconds": args.delivery_elapsed_seconds,
        "publishRatePerSecond": (
            args.messages / args.elapsed_seconds
            if args.elapsed_seconds > 0
            else 0.0
        ),
        "deliveryRatePerSecond": (
            received / args.delivery_elapsed_seconds
            if args.delivery_elapsed_seconds > 0
            else 0.0
        ),
        "latencySamples": len(latencies),
        "latencyCoverage": coverage,
        "latencyP50Milliseconds": percentile(latencies, 0.50),
        "latencyP95Milliseconds": percentile(latencies, 0.95),
        "latencyP99Milliseconds": percentile(latencies, 0.99),
        "latencyMaxMilliseconds": max(latencies) if latencies else None,
        "phases": phases,
        "brokerResources": broker_resources,
        "clientResources": combine_client_resources(
            subscriber_resources,
            publisher_resources,
        ),
        "mqttZeroMemory": (
            load_mqtt_zero_memory(args.broker_log)
            if args.broker == "mqttzero"
            else None
        ),
    }

    Path(args.output).write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(args.markdown_output, result)


if __name__ == "__main__":
    main()
