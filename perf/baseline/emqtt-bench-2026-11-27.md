# eMQTT-Bench MQTTZero / MQTTnet Comparison

## Decision

This run establishes the eMQTT-Bench workflow and development evidence only.
The current Tencent host has two vCPUs; none of these values is a capacity or
universal performance claim.

The three throughput cells did not pass the `MQTTZero >= MQTTnet` gate. The
targeted latency cell passed its p99 gate.

## Environment

| Item | Value |
|---|---|
| Date | 2026-11-27 |
| Host | Tencent Cloud Ubuntu 24.04.4 LTS |
| CPU | 2 vCPU |
| Memory | 3.6 GiB |
| .NET SDK | 10.0.115 |
| .NET Runtime | 10.0.15 |
| Broker commit | `5ef39ac` |
| Broker publish | Linux x64 framework-dependent |
| Client | eMQTT-Bench 0.6.4 |
| Client package SHA-256 | `7b6a1a8db3a96601be36370e321b6169126047f0d30c9d30b64cec39e69746dd` |
| Broker payload SHA-256 | `a85023595261bb6cd49ade5b34cce70fdc2764629037650b731533ac0aa51951` |

Both brokers use Kestrel and MQTT 5. The client and broker run as separate
processes on the same host.

## Commands

```bash
REPETITIONS=3 START_PORT=19000 \
  ./run-matrix.sh throughput results/q1-500-64k 1 500 65536

REPETITIONS=3 START_PORT=19020 \
  ./run-matrix.sh throughput results/q1-100-8k 1 100 8192

REPETITIONS=3 START_PORT=19040 \
  ./run-matrix.sh throughput results/q0-1-32 0 1 32

REPETITIONS=1 START_PORT=19060 \
  ./run-matrix.sh latency results/q1-1-8k-latency 1 1 8192
```

Throughput message counts are 5000 for 32 B, 1000 for 8 KiB and 100 for 64 KiB.
The latency profile sends 500 messages after 50 warm-up messages with a 20 ms
publish interval.

## Throughput Results

Values are three-run medians. CPU is measurement-phase average percentage of
one core. RSS and PSS are peak values from 100 ms `/proc` samples.

| Cell | MQTTZero delivery/s | MQTTnet delivery/s | Ratio | CPU % (zero/net) | Peak RSS MB (zero/net) | Peak PSS MB (zero/net) | Result |
|---|---:|---:|---:|---:|---:|---:|---|
| QoS1 / 500 / 64 KiB | 3896.2 | 4300.4 | 0.906 | 27.0 / 24.8 | 479.7 / 202.6 | 468.7 / 193.6 | FAIL |
| QoS1 / 100 / 8 KiB | 16988.9 | 17064.1 | 0.996 | 55.7 / 44.0 | 145.0 / 158.1 | 136.1 / 149.3 | FAIL |
| QoS0 / 1 / 32 B | 462.2 | 463.4 | 0.997 | 18.9 / 18.3 | 72.5 / 68.8 | 63.7 / 60.1 | FAIL |

The corresponding p99 values were:

| Cell | MQTTZero p99 | MQTTnet p99 | Ratio | Latency coverage |
|---|---:|---:|---:|---|
| QoS1 / 500 / 64 KiB | 11249 ms | 10372 ms | 1.085 | complete |
| QoS1 / 100 / 8 KiB | 3821 ms | 3605 ms | 1.060 | complete |
| QoS0 / 1 / 32 B | 1 ms | 1 ms | 1.000 | incomplete; not a latency gate |

## Latency Result

| Cell | MQTTZero p99 | MQTTnet p99 | Ratio | CPU % (zero/net) | Peak RSS MB (zero/net) | Result |
|---|---:|---:|---:|---:|---:|---|
| QoS1 / 1 / 8 KiB | 3 ms | 4 ms | 0.750 | 6.4 / 4.3 | 70.3 / 69.7 | PASS |

Both runs reported complete delivery and at least 99 percent QoE sample
coverage.

## eMQTT-Bench QoS 0 Notes

- eMQTT-Bench 0.6.4 counts QoS 0 publications once in its worker loop. The
  source explicitly avoids incrementing the publish-success counter again on
  the QoS 0 publish path.
- The runner never uses the publisher's attempted/sent count as broker
  delivery. `receivedDeliveries` is derived from subscriber receipt and total
  completeness is checked against `messages × subscribers`.
- eMQTT-Bench 0.6.4 completed QoS 0 publishing at `-I 0` and `-I 1` while the
  subscriber received no messages. The matrix therefore uses `-I 2` for QoS 0
  throughput cells. QoS 1 and QoS 2 remain unthrottled in throughput mode.
- QoS 0 has no acknowledgement or retransmission. A dropped QoS 0 delivery is
  reported as incomplete and is never repaired by the runner.

## Raw Diagnostic Client

`RawMqttClient` and all of its tests remain unchanged. It is still used for:

- microsecond latency histograms;
- sequence, duplicate and missing-delivery detection;
- paused-reader backpressure and buffer ownership tests;
- benchmark-local correctness diagnostics.

It is not the final independent-client comparison.

## Raw Results

- `perf/baseline/emqtt-bench-2026-11-27/acceptance-q1-500-64k/`
- `perf/baseline/emqtt-bench-2026-11-27/acceptance-q1-100-8k/`
- `perf/baseline/emqtt-bench-2026-11-27/acceptance-q0-1-32/`
- `perf/baseline/emqtt-bench-2026-11-27/acceptance-latency-q1-1-8k/`

Each directory contains `summary.json`, `summary.md`, `runs.tsv`, per-run
summaries and phase timestamps.
