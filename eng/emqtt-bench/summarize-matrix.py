#!/usr/bin/env python3
import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def resource_medians(
    results: list[dict[str, Any]],
    section: str,
    key: str,
) -> float | None:
    values = [
        float(result[section][key])
        for result in results
        if result.get(section) and result[section].get(key) is not None
    ]
    return median(values)


def summarize_cell(
    mode: str,
    qos: int,
    subscribers: int,
    payload: int,
    repetitions: int,
    by_broker: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    brokers: dict[str, Any] = {}
    for broker in ("mqttzero", "mqttnet"):
        results = by_broker.get(broker, [])
        complete = len(results) == repetitions and all(
            result.get("throughputValid", False) for result in results
        )
        latency_complete = len(results) == repetitions and all(
            result.get("latencyValid", False) for result in results
        )
        brokers[broker] = {
            "runCount": len(results),
            "complete": complete,
            "latencyComplete": latency_complete,
            "publishRatePerSecond": median(
                [result["publishRatePerSecond"] for result in results]
            ),
            "deliveryRatePerSecond": median(
                [result["deliveryRatePerSecond"] for result in results]
            ),
            "latencyP50Milliseconds": median(
                [
                    result["latencyP50Milliseconds"]
                    for result in results
                    if result.get("latencyP50Milliseconds") is not None
                ]
            ),
            "latencyP95Milliseconds": median(
                [
                    result["latencyP95Milliseconds"]
                    for result in results
                    if result.get("latencyP95Milliseconds") is not None
                ]
            ),
            "latencyP99Milliseconds": median(
                [
                    result["latencyP99Milliseconds"]
                    for result in results
                    if result.get("latencyP99Milliseconds") is not None
                ]
            ),
            "brokerCpuMilliseconds": resource_medians(
                results,
                "brokerResources",
                "totalCpuMilliseconds",
            ),
            "brokerAveragePercentOfOneCore": resource_medians(
                results,
                "brokerResources",
                "averagePercentOfOneCore",
            ),
            "brokerPeakResidentBytes": resource_medians(
                results,
                "brokerResources",
                "peakResidentBytes",
            ),
            "brokerPeakPssBytes": resource_medians(
                results,
                "brokerResources",
                "peakPssBytes",
            ),
            "brokerPeakThreadCount": resource_medians(
                results,
                "brokerResources",
                "peakThreadCount",
            ),
            "clientCpuMilliseconds": resource_medians(
                results,
                "clientResources",
                "totalCpuMilliseconds",
            ),
        }

    mqtt_zero = brokers["mqttzero"]
    mqtt_net = brokers["mqttnet"]
    delivery_ratio = (
        mqtt_zero["deliveryRatePerSecond"] / mqtt_net["deliveryRatePerSecond"]
        if mqtt_zero["deliveryRatePerSecond"]
        and mqtt_net["deliveryRatePerSecond"]
        else None
    )
    p99_ratio = (
        mqtt_zero["latencyP99Milliseconds"]
        / mqtt_net["latencyP99Milliseconds"]
        if mqtt_zero["latencyP99Milliseconds"]
        and mqtt_net["latencyP99Milliseconds"]
        else None
    )
    p95_ratio = (
        mqtt_zero["latencyP95Milliseconds"]
        / mqtt_net["latencyP95Milliseconds"]
        if mqtt_zero["latencyP95Milliseconds"]
        and mqtt_net["latencyP95Milliseconds"]
        else None
    )
    complete = mqtt_zero["complete"] and mqtt_net["complete"]
    latency_complete = (
        mqtt_zero["latencyComplete"] and mqtt_net["latencyComplete"]
    )
    if mode == "throughput":
        throughput_gate_passed = bool(
            complete
            and delivery_ratio is not None
            and delivery_ratio >= 1.0
        )
        tail_gate_passed = None
        latency_gate_eligible = False
    else:
        throughput_gate_passed = None
        latency_gate_eligible = latency_complete
        tail_gate_passed = bool(
            complete
            and latency_complete
            and p99_ratio is not None
            and p99_ratio <= 1.0
        )
    gate_passed = (
        throughput_gate_passed
        if mode == "throughput"
        else tail_gate_passed
    )

    return {
        "qos": qos,
        "subscribers": subscribers,
        "payloadBytes": payload,
        "mode": mode,
        "deliveryThroughputRatio": delivery_ratio,
        "p95Ratio": p95_ratio,
        "p99Ratio": p99_ratio,
        "throughputGatePassed": throughput_gate_passed,
        "latencyGateEligible": latency_gate_eligible,
        "tailGatePassed": tail_gate_passed,
        "gatePassed": gate_passed,
        "brokers": brokers,
    }


def write_markdown(path: str, mode: str, cells: list[dict[str, Any]]) -> None:
    lines = [
        f"# eMQTT-Bench {mode} matrix",
        "",
        "Development evidence on the current host; not a capacity claim.",
        "",
        "| QoS | Subs | Payload | MQTTZero delivery/s | MQTTnet delivery/s | Ratio | MQTTZero p95 ms | MQTTnet p95 ms | P95 ratio (diagnostic) | MQTTZero p99 ms | MQTTnet p99 ms | P99 ratio | Broker CPU % (zero/net) | Broker peak RSS MB (zero/net) | Passed |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for cell in cells:
        zero = cell["brokers"]["mqttzero"]
        net = cell["brokers"]["mqttnet"]

        def format_value(value: float | None, digits: int) -> str:
            return "n/a" if value is None else f"{value:.{digits}f}"

        cpu_zero = format_value(zero["brokerAveragePercentOfOneCore"], 1)
        cpu_net = format_value(net["brokerAveragePercentOfOneCore"], 1)
        rss_zero = format_value(
            (
                zero["brokerPeakResidentBytes"] / (1024 * 1024)
                if zero["brokerPeakResidentBytes"] is not None
                else None
            ),
            1,
        )
        rss_net = format_value(
            (
                net["brokerPeakResidentBytes"] / (1024 * 1024)
                if net["brokerPeakResidentBytes"] is not None
                else None
            ),
            1,
        )
        lines.append(
            "| "
            f"{cell['qos']} | {cell['subscribers']} | {cell['payloadBytes']} | "
            f"{format_value(zero['deliveryRatePerSecond'], 1)} | "
            f"{format_value(net['deliveryRatePerSecond'], 1)} | "
            f"{format_value(cell['deliveryThroughputRatio'], 3)} | "
            f"{format_value(zero['latencyP95Milliseconds'], 1)} | "
            f"{format_value(net['latencyP95Milliseconds'], 1)} | "
            f"{format_value(cell['p95Ratio'], 3)} | "
            f"{format_value(zero['latencyP99Milliseconds'], 1)} | "
            f"{format_value(net['latencyP99Milliseconds'], 1)} | "
            f"{format_value(cell['p99Ratio'], 3)} | "
            f"{cpu_zero}/{cpu_net} | {rss_zero}/{rss_net} | "
            f"{cell['gatePassed']} |"
        )
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown-output", required=True)
    parser.add_argument("--mode", required=True, choices=("throughput", "latency"))
    parser.add_argument("--repetitions", required=True, type=int)
    args = parser.parse_args()

    grouped: dict[
        tuple[int, int, int],
        dict[str, list[dict[str, Any]]],
    ] = defaultdict(lambda: defaultdict(list))
    with Path(args.runs).open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source, delimiter="\t"):
            result = json.loads(
                Path(row["summary"]).read_text(encoding="utf-8")
            )
            key = (
                int(row["qos"]),
                int(row["subscribers"]),
                int(row["payload"]),
            )
            grouped[key][row["broker"]].append(result)

    cells = [
        summarize_cell(
            args.mode,
            qos,
            subscribers,
            payload,
            args.repetitions,
            by_broker,
        )
        for (qos, subscribers, payload), by_broker in sorted(grouped.items())
    ]
    document = {
        "mode": args.mode,
        "repetitions": args.repetitions,
        "developmentEvidenceOnly": True,
        "gatePassed": all(cell["gatePassed"] for cell in cells),
        "cells": cells,
    }
    Path(args.output).write_text(
        json.dumps(document, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(args.markdown_output, args.mode, cells)


if __name__ == "__main__":
    main()
