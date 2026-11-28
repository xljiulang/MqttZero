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
- An approved `host.json` profile passed through `HOST_PROFILE`

The default remote paths are:

```text
/home/ubuntu/mqttzero-dev/tools/emqtt-bench/0.6.4/bin/emqtt_bench
/home/ubuntu/mqttzero-dev/artifacts/broker-linux-x64/current/runtime
```

Override them with `EMQTT_BENCH_BIN`, `MQTTZERO_BROKER_DIR` and
`MQTTZERO_DEV_ROOT`.

## Host approval

Every run validates the current host before starting either broker. Generate
the approved profile once on the Linux development server:

```bash
export MQTTZERO_BENCH_COMMIT=$(git rev-parse HEAD)
python3 host_preflight.py \
  --host-id linux-dev-2vcpu \
  --expected-cpus 2 \
  --expected-min-memory-gib 3 \
  --expected-fd-limit 1048576 \
  --output /home/ubuntu/mqttzero-dev/artifacts/linux-dev-host.json
```

When the benchmark runs from a source archive rather than a Git checkout, the
caller must set `MQTTZERO_BENCH_COMMIT` to the exact archived commit.

Pass that file to every cell:

```bash
HOST_PROFILE=/home/ubuntu/mqttzero-dev/artifacts/linux-dev-host.json \
  ./run-cell.sh mqttzero 18841 1 100 8192 200 20 /tmp/emqtt-cell
```

The runner rejects a host whose identity, CPU model, logical CPU count, memory,
OS, .NET version, eMQTT version or file-descriptor limit differs from the
approved profile.

## Single cell

```bash
./run-cell.sh <broker> <port> <qos> <subscribers> <payload> <messages> \
    <warmup-messages> <output-dir>
```

Example:

```bash
./run-cell.sh mqttzero 18841 1 100 8192 200 20 /tmp/emqtt-cell-mqttzero
```

## Linux development server campaign

Run a complete campaign from a clean checkout:

```powershell
./run-linux-dev.ps1 `
  -SshConfigPath $env:MQTTZERO_SSH_CONFIG `
  -Mode throughput `
  -Qos 1 `
  -Subscribers 500 `
  -Payload 65536 `
  -Repetitions 3 `
  -StartPort 19000
```

The runner validates a clean worktree, archives the exact commit, builds and
publishes a portable framework-dependent build on the Linux development
server, runs the host preflight, executes the matrix there, verifies the host
fingerprint again afterward and copies the campaign artifacts into
`artifacts/emqtt-bench/`.

Use `-DryRun` to inspect the selected host and matrix without contacting the
server. Use `-SmokeMessages` to run a single-cell smoke on both brokers.

Use `-Quick` for a single-repetition throughput cell. It reduces the QoE flush
wait to one second, so it is suitable for rapid trend checks and allocation or
correctness smoke tests. It does not produce a valid p99 conclusion.

The campaign runner forwards these optional benchmark variables:

```text
MAX_BUFFERED_BYTES
OUTBOUND_BATCH_BYTES
OUTBOUND_BATCH_MESSAGES
OUTBOUND_BATCH_DURATION_US
EVENT_LOOP_SHARDS
IO_QUEUE_COUNT
ENABLE_FANOUT_DIAGNOSTICS
ENABLE_MQTTZERO_MEMORY_PROBE
MEMORY_PROBE_MS
MAX_PENDING
PROCESS_SAMPLE_MS
QOE_FLUSH_SECONDS
```

`run-cell.sh` writes the effective values and broker arguments to
`benchmark-config.json`. The content is embedded in `summary.json` under
`benchmarkConfiguration`.

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

Throughput message counts are reduced as subscriber fanout grows so one cell
does not generate millions of deliveries on the two-vCPU development host:

| Payload | 1 subscriber | 100 subscribers | 500 subscribers |
|---|---:|---:|---:|
| 32 B | 5000 | 1000 | 200 |
| 8 KiB | 1000 | 200 | 50 |
| 64 KiB | 100 | 100 | 100 |

The latency profile sends 500 messages with a 20 ms publish interval and a
50-message warm-up. This avoids eMQTT-Bench QoE loss from receiving multiple
messages within the same millisecond and leaves enough samples to satisfy the
99 percent coverage gate.

For QoS 0, eMQTT-Bench 0.6.4 can finish publishing while the subscriber receives
nothing at `-I 0` or `-I 1`. The matrix therefore uses a 2 ms interval for QoS 0
throughput cells. QoS 1 and QoS 2 remain unthrottled in throughput mode.

QoS 0 remains diagnostic-only for cross-broker performance claims. At larger
fanout or payload sizes eMQTT-Bench 0.6.4 can still report incomplete delivery,
so formal throughput gates use QoS 1 while internal Raw diagnostics cover QoS 0
ordering and allocation behavior.

The `0.6.4` source counts QoS 0 publications once in the worker loop and no
longer double-counts the successful publish path associated with `0.6.3`.
The runner still treats `receivedDeliveries` as the authoritative Broker
delivery count and never substitutes the publisher's attempted/sent counter.

`summary.json` and `summary.md` include throughput ratio, p99 ratio, broker CPU
and peak RSS.

Throughput cells gate delivery throughput and completeness only. Their
p50/p95/p99 values are backlog-completion diagnostics. Latency cells require
at least 99 percent QoE coverage; p95 is reported as an early-warning metric,
while p99 remains the formal tail-latency gate.

`run-cell.sh` can also be started with `PUBLISH_INTERVAL_MS=2` directly.

## Tests

```bash
./run-tests.sh
```

## Scope

The current cloud development host has two vCPUs. Use it for tool validation
and trend checks. A capacity claim requires a larger isolated host or separate
client and broker machines.
