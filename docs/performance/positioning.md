# MQTTZero Performance Positioning

> 上级索引：[文档目录](../README.md)

MQTTZero is an MQTT 5 broker for ASP.NET Core Kestrel. Its release quality is
defined by protocol correctness, bounded resources, zero allocation in selected
synchronized hot paths and stable operation.

## Supported Strengths

- MQTT 5 protocol completeness, including QoS 1/2, sessions, retained messages,
  wills, message expiry and property semantics.
- Zero-allocation fast paths for PUBLISH decode, route planning, QoS queues,
  acknowledgement decoding and memory transport.
- Shared reference-counted payload ownership and bounded pooled memory.
- Kestrel TCP, TLS and WebSocket integration with ASP.NET Core dependency
  injection.
- Routing and retained-message indexes designed for subscription rebuilds.

## Release Gate

An ordinary alpha release requires all correctness tests, allocation sentinels,
real transport tests and the Dashboard tests to pass. It also requires that no
benchmark cell regress throughput by more than five percent from the
same-machine stable baseline, with zero failed operations, connection failures,
drain timeouts or lease leaks.

## Performance-Claim Gate

The nightly comparison against MQTTnet is intentionally stricter. A cell may be
described as ahead of MQTTnet only after three repetitions show higher median
throughput and no worse p99. The strict matrix does not block an ordinary alpha
release.

Known gaps include large-payload high-fanout throughput and tail latency. Those
cells are recorded in `perf/baseline` and are not implied by the general
zero-allocation claim.

## Shard-Owned Egress Status

The design described above is now part of `master`. The
`refactor/shard-owned-egress-timers` branch delivered:

- one `MqttZeroSessionEgress` as the sole outbound owner for each Session;
- one four-level hierarchical timer wheel per Shard;
- Shard-confined Session mutation with lock-free management snapshots;
- removal of Session `SyncRoot`, mutation locks and mutation leases.

Correctness, allocation, Release build, formatting, supply-chain and NuGet
consumer gates pass. The merge was held for a while by sub-95-percent retained
throughput on several development-matrix cells. The regression came from the
cross-Shard command allocation on the session mutation path and from per-frame
control flushing; both were repaired before the merge. The earlier
investigation is recorded in
`perf/baseline/shard-owned-egress-ab-investigation-2026-11-26.md` and
`perf/baseline/shard-owned-egress-control-diag-2026-11-27.md`.

The final alternating same-machine verdict (`master@ec8ba3f` versus
`refactor/shard-owned-egress-timers@6820982`, three rounds, 5 s warm-up and
20 s measurement) is recorded in
`perf/baseline/shard-owned-egress-final2-2026-11-27.md`:

| Cell | Throughput | Delivery p99 ratio | Verdict |
|---|---:|---:|---|
| QoS 1 / 100 / 64 KiB | +1.5% | 0.835 | pass |
| QoS 2 / 100 / 8 KiB | +3.4% | 0.885 | pass |
| QoS 0 / 100 / 32 B | 0.0% | 1.000 | pass |
| QoS 0 / 100 / 8 KiB | +9.0% | 0.888 | pass |

QoS 1 / 1 / 64 KiB only becomes decidable after the in-flight window is reduced
to eight: the end-to-end five-round ratio is `0.961`. The earlier `1.243` came
from a three-round sample whose own baseline was noisy; the per-round ratio
median over eight rounds is `0.999`.

The merge closed the regression against MQTTZero's own baseline. It did not
establish leadership over MQTTnet, and no such claim is made.

## MQTTnet Comparison Status

The final external comparison client is now eMQTT-Bench `0.6.4`. The Linux
runner records end-to-end throughput, a throttled QoE latency profile, broker
and client CPU, peak RSS, peak PSS and thread count. Results from the current
two-vCPU development host are development evidence only.

The benchmark-only Raw client remains the internal diagnostic client for
microsecond latency, sequence integrity and paused-reader backpressure. It is
not used as the final independent-client comparison.

Three same-machine comparisons used 8 logical cores:

- `perf/baseline/compare-8core-2026-11-26.md` (MQTTnet client): all three cells
  below the strict gate (`0.973`, `0.866`, `0.865`).
- `perf/baseline/compare-8core-2026-11-27.md` (MQTTnet client): two cells at or
  above parity (`1.040`, `1.029`), QoS 1 / 100 / 64 KiB behind (`0.896`).
- `perf/baseline/compare-8core-raw-client-2026-11-27.md` (raw client): `0.981`,
  `0.998` and `0.909` throughput with delivery p99 ratios of `0.504`, `0.692`
  and `0.385`.

The load client is a first-order variable. Switching only the client from
MQTTnet to the benchmark-only raw client raised single-broker throughput by
1.7–2.6x and moved all three ratios toward 1.0. The “two cells above parity”
result therefore reflects the MQTTnet client's per-message cost rather than a
broker advantage. Latency is consistent across both clients: MQTTZero keeps a
lower delivery p99 in every raw-client cell.

MQTTnet's own absolute throughput also moved 16–46 percent between 11-26 and
11-27 on an unchanged MQTTnet build, so no cross-date comparison is valid. No
sample satisfies the three-repetition leadership gate; treat every record as
dated, same-machine evidence only.

## Next Performance Step

No further refactoring is planned on the shard or egress scheduling paths. The
remaining gaps need new evidence first:

1. QoS 1 / 100 / 64 KiB is behind MQTTnet in all three samples (`0.896`,
   `0.909`). Re-measure it on a quiet machine with more repetitions, and with a
   client whose per-message cost is not the dominant term, before changing code.
2. Keep the pooled QoS0 and PUBACK paths unchanged unless a repeatable
   exception is found.
3. Do not pipeline QoS2 PUBLISH unless the full PUBREC/PUBREL persistence and
   ordering suite remains green.
4. Do not reopen Fast Fanout or the Publish Sequencer until routing or fanout
   is measured as the bottleneck. The recorded diagnostics point at control
   frame batching and per-message allocation instead (see
   `docs/performance/hot-path-audit.md`).

Development regression checks may use two repetitions to keep iteration cost
bounded. Final performance claims and published comparison matrices still
require three repetitions. A two-run development sample must be repeated with
a third run only when end-to-end throughput spread exceeds ten percent or a
run has failures or incomplete delivery. P99 is recorded in development but
does not trigger a third run because short-run tail noise is too high.

Universal MQTTnet superiority across every benchmark cell remains outside the
ordinary release gate. Performance claims are limited to measured cells and
named metrics.

## Current Decision

The Shard-owned egress work is merged and the ordinary release gate is green.
The branch is no longer blocked and no compatibility shim is planned.
Performance claims stay limited to the four passing cells of the final A/B.
The QoS 1 / 100 / 64 KiB gap against MQTTnet is tracked in
`perf/baseline/compare-8core-2026-11-27.md`.

## 2026-11-28 Linux Development Gate

The current convergence branch is split into two review units:

1. Linux benchmark harness and methodology.
2. Egress diagnostics and large-message batch behavior.

The target cell `QoS1 / 500 / 64 KiB` passes the strict five-run gate:

- throughput: `1.082x` MQTTnet;
- p95: `0.985x`;
- p99: `0.991x`;
- peak RSS: `0.949x`.

The branch is nevertheless blocked from merge because the latency protection
cell `QoS1 / 100 / 8 KiB` has a five-run p99 median of `1.125x` MQTTnet.
The target-cell result is recorded in
`perf/baseline/linux-dev-mqttnet-release-gate.md`.
