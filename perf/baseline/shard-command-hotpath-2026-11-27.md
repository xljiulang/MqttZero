# MQTTZero Cross-Shard Publish And ACK Hotpath Result

## Decision

**Status: PARTIAL RECOVERY, STILL BLOCKED BY PERFORMANCE AND TAIL-LATENCY GATES.**

The implementation removes the dominant `d14f433` allocation and scheduling
regression for the normal QoS 0 and QoS 1 paths:

- no `MqttZeroShardInvocation<T>` or `TaskCompletionSource<T>` is created for
  network QoS 0 or QoS 1 PUBLISH;
- simple QoS 0 publishes route from the connection thread and schedule all
  target batches through Fanout Schedulers;
- complex QoS 0 publishes and PUBACK processing use bounded pooled shard work;
- QoS 1 performs its ACK-front preparation on the connection thread and sends
  PUBACK after the inbound work is queued;
- PUBREC/PUBREL/PUBCOMP retain ordered completion semantics through the reusable
  shard command fallback.

The same-machine Broker memory probe now matches the pre-regression range:

| Commit | Wave 2 allocation delta | Wave 3 allocation delta |
|---|---:|---:|
| `d424810` before regression | 420,168 B | 330,392 B |
| Current repaired path | 408,848 B | 444,984 B |

The branch is nevertheless not mergeable because QoS0 tails and QoS2 tails
remain above the five-percent gate.

## Environment

| Item | Value |
|---|---|
| Date | 2026-11-27 |
| CPU | 12th Gen Intel Core i5-12450H, 8 cores / 12 logical processors |
| OS | Windows `10.0.26300`, x64 |
| Runtime | .NET `10.0.15` |
| SDK | .NET SDK `10.0.400` |
| A/B client | MQTTnet Client |
| Broker | Kestrel, Server GC, Concurrent GC, TieredPGO |
| Window | 2 s warmup, 6 s measurement, 3 rounds plus two for spread |
| Baseline | `d424810`, before the cross-shard mutation regression |

## Final Performance Matrix

| QoS | Subscribers | Payload | Rounds | Baseline msg/s | Candidate msg/s | Throughput | Baseline p99 us | Candidate p99 us | p99 ratio |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 B | 5 | 30,905.11 | 32,489.87 | 1.051x | 34,922 | 37,282 | 1.068x |
| 0 | 1 | 8 KiB | 5 | 3,569.22 | 3,542.97 | 0.993x | 595,082 | 901,045 | 1.514x |
| 0 | 100 | 32 B | 5 | 541.42 | 588.64 | 1.087x | 120,787 | 150,631 | 1.247x |
| 0 | 100 | 8 KiB | 5 | 152.40 | 143.42 | 0.941x | 585,652 | 422,655 | 0.722x |
| 1 | 1 | 32 B | 3 | 1,650.76 | 1,674.63 | 1.014x | 1,164 | 1,139 | 0.979x |
| 1 | 1 | 8 KiB | 3 | 1,425.27 | 1,434.34 | 1.006x | 1,363 | 1,325 | 0.972x |
| 1 | 100 | 32 B | 3 | 89.91 | 91.58 | 1.019x | 15,516 | 13,076 | 0.843x |
| 1 | 100 | 8 KiB | 3 | 68.84 | 67.18 | 0.976x | 18,212 | 19,119 | 1.050x |
| 2 | 1 | 32 B | 5 | 860.22 | 837.31 | 0.973x | 1,971 | 2,312 | 1.173x |
| 2 | 1 | 8 KiB | 5 | 804.20 | 774.86 | 0.964x | 2,127 | 2,276 | 1.070x |
| 2 | 100 | 32 B | 3 | 45.97 | 46.63 | 1.014x | 26,717 | 29,261 | 1.095x |
| 2 | 100 | 8 KiB | 5 | 39.16 | 37.28 | 0.952x | 33,670 | 48,646 | 1.445x |
| 0 | 1 | 64 KiB | 5 | 1,041.34 | 1,182.15 | 1.135x | 1,576,900 | 1,608,438 | 1.020x |
| 0 | 100 | 64 KiB | 3 | 23.37 | 24.30 | 1.040x | 2,343,190 | 2,039,888 | 0.870x |
| 1 | 1 | 64 KiB | 3 | 768.85 | 768.86 | 1.000x | 318,804 | 323,544 | 1.015x |
| 1 | 100 | 64 KiB | 5 | 18.56 | 18.40 | 0.991x | 67,525 | 68,909 | 1.020x |

All cells had zero failed operations and zero missing deliveries.

## Gate Result

Passed:

- QoS 1 throughput and p99 across 1/100 subscribers at 32 B/8 KiB.
- QoS 1 64 KiB throughput and p99.
- QoS 0 64 KiB throughput and p99.
- QoS 2 throughput is within five percent of the baseline in all measured
  development cells.

Blocked:

- QoS 0 / 1 / 8 KiB p99 `1.514x`.
- QoS 0 / 100 / 32 B p99 `1.247x`.
- QoS 0 / 100 / 8 KiB throughput `0.941x`.
- QoS 2 / 1 / 32 B p99 `1.173x`.
- QoS 2 / 1 / 8 KiB p99 `1.070x`.
- QoS 2 / 100 / 32 B p99 `1.095x`.
- QoS 2 / 100 / 8 KiB p99 `1.445x`.

QoS 2 PUBLISH cannot be made fire-and-forget safely: an attempted pooled-work
experiment broke PUBREC/PUBREL persistence and ordering tests and was removed.
QoS 2 remains on the synchronous reusable command path.

## Correctness And Build Gates

| Gate | Result |
|---|---|
| Release solution build | PASS, 0 warnings / 0 errors |
| Core tests | PASS, 761/761 |
| Broker benchmark tests | PASS, 71/71 |
| Allocation sentinels | PASS, 23/23 |
| Format verification | PASS |
| Vulnerable packages | PASS, none |
| Deprecated packages | PASS, none |
| NuGet pack | PASS, all packages |
| Repository-external consumers | PASS, 0 warnings / 0 errors |

## Retained Implementation

- Reusable internal `MqttZeroShardCommand<T>` for ordered fallback work.
- Pooled QoS 0 publish and PUBACK work with bounded per-connection drain.
- Direct QoS 0 fast route for no-store/no-interceptor/no-observer/no-retain
  traffic.
- Direct QoS 1 preparation and queued inbound dispatch before PUBACK.
- Cached connection delegates and no per-message delegate allocation.
- Connection cleanup waits for queued QoS 0 and PUBACK work.
- ShardSet shutdown now stops Fanout Schedulers before Egress Schedulers, so
  producer work drains before the consumer is stopped.
- QoS2 publish fire-and-forget, QoS2 PUBLISH work, and bulk asynchronous
  PUBREC/PUBREL/PUBCOMP experiments were deleted after correctness or ordering
  failures.

## Raw Results

Raw A/B JSON is stored in:

`artifacts/broker-ab/shard-owned-regression-2026-11-26/fix-validation/`

Committed evidence is stored in:

`perf/baseline/shard-command-hotpath-raw-2026-11-27/`

## Next Action

Do not merge or raise the version. The next investigation should use longer
end-to-end runs for the remaining QoS0 and QoS2 p99 cells to separate real tail
work from short-run outliers. Any further QoS2 pipeline experiment must retain
the full PUBREC/PUBREL persistence ordering suite.
