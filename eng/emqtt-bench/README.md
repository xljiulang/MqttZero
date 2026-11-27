# eMQTT-Bench comparison runner

This runner compares the MQTTZero and MQTTnet benchmark brokers with the same
external MQTT 5 client. It is intended for development and regression checks on
the Tencent Linux development machine.

## Requirements

- eMQTT-Bench `0.6.4`
- A framework-dependent Linux x64 broker publish, or a runtime-carrying publish
- .NET 10 runtime when using the framework-dependent publish
- Linux `/proc`
- Python 3.10 or later

The default remote paths are:

```text
/home/ubuntu/mqttzero-dev/tools/emqtt-bench/0.6.4/bin/emqtt_bench
/home/ubuntu/mqttzero-dev/artifacts/broker-linux-x64/current/runtime
```

Override them with `EMQTT_BENCH_BIN`, `MQTTZERO_BROKER_DIR` and
`MQTTZERO_DEV_ROOT`.

## Single cell

```bash
./run-cell.sh <broker> <port> <qos> <subscribers> <payload> <messages> \
    <warmup-messages> <output-dir>
```

Example:

```bash
./run-cell.sh mqttzero 18841 1 100 8192 200 20 /tmp/emqtt-cell-mqttzero
```

Use `PUBLISH_INTERVAL_MS=0` for an unthrottled throughput run. Use
`PUBLISH_INTERVAL_MS=2` for the latency profile: eMQTT-Bench QoE uses
millisecond-resolution keys, so a throttled run avoids same-millisecond
overwrites and preserves a complete per-delivery latency sample set.

The output directory contains:

- `summary.json`
- `summary.md`
- `phases.json`
- `broker-samples.csv`
- `subscriber-samples.csv`
- `publisher-samples.csv`
- Broker, subscriber and publisher logs
- eMQTT-Bench QoE disk log and CSV

`summary.json` reports publish rate, end-to-end subscriber delivery rate,
delivery completeness, latency sample coverage and p50/p95/p99 from the QoE
CSV. `publishRatePerSecond` uses the publisher call duration, while
`deliveryRatePerSecond` uses the time from measurement start through subscriber
drain.

The runner uses the `ts` payload header only. A `cnt64` header cannot be used
across separate warm-up and measurement publisher processes because each
publisher restarts its counter at zero.

The publisher clock starts at eMQTT-Bench's `start publishing` marker, so
CONNECT time is excluded from the measurement window.

The runner waits one QoE reporting interval after the target delivery count is
reached before stopping the subscriber. Without that wait, the final QoE bucket
can remain unflushed and latency coverage will be incomplete.

The default flush wait is 15 seconds. The summary marks a run valid only when
all expected deliveries were observed and at least 99 percent of those
deliveries have a QoE latency sample.

Unthrottled runs keep `throughputValid=true` when delivery counts are complete
even if `latencyValid=false`. Use the throttled latency profile for p99 gates.

## Resource observation

`sample-process.py` reads `/proc/<pid>/stat`, `/proc/<pid>/status` and
`/proc/<pid>/smaps_rollup` every 100 ms. `summary.json` reports broker and
client CPU, peak RSS, peak PSS, private dirty memory, thread count and context
switches. CPU per 1000 publications/deliveries is calculated from the
measurement phase.

Set `ENABLE_MQTTZERO_MEMORY_PROBE=1` to pass
`--memory-probe-ms 100` to the MQTTZero broker. Parsed `MEMORYPROBE` peaks are
stored under `mqttZeroMemory`. Enable this only for diagnostics; it is not part
of the fair cross-broker performance profile.

## Matrix

```bash
./run-matrix.sh throughput artifacts/emqtt-bench/throughput
./run-matrix.sh latency artifacts/emqtt-bench/latency
```

The matrix defaults to QoS 0/1/2, subscribers 1/100/500 and payloads
32 B/8 KiB/64 KiB. Each cell runs three times with alternating broker order.
Invalid runs are retried once.

Throughput message counts are 5000 for 32 B, 1000 for 8 KiB and 100 for 64 KiB.
The latency profile sends 500 messages with a 20 ms publish interval and a
50-message warm-up. This avoids eMQTT-Bench QoE loss from receiving multiple
messages within the same millisecond and leaves enough samples to satisfy the
99 percent coverage gate.

For QoS 0, eMQTT-Bench 0.6.4 can finish publishing while the subscriber receives
nothing at `-I 0` or `-I 1`. The matrix therefore uses a 2 ms interval for QoS 0
throughput cells. QoS 1 and QoS 2 remain unthrottled in throughput mode.

The `0.6.4` source counts QoS 0 publications once in the worker loop and no
longer double-counts the successful publish path associated with `0.6.3`.
The runner still treats `receivedDeliveries` as the authoritative Broker
delivery count and never substitutes the publisher's attempted/sent counter.

`summary.json` and `summary.md` include throughput ratio, p99 ratio, broker CPU
and peak RSS.

`run-cell.sh` can also be started with `PUBLISH_INTERVAL_MS=2` directly.

## Tests

```bash
./run-tests.sh
```

## Scope

The current cloud development host has two vCPUs. Use it for tool validation
and trend checks. A capacity claim requires a larger isolated host or separate
client and broker machines.
