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

## Current Shard-Owned Egress Status

The `refactor/shard-owned-egress-timers` branch implements:

- one `MqttZeroSessionEgress` as the sole outbound owner for each Session;
- one four-level hierarchical timer wheel per Shard;
- Shard-confined Session mutation with lock-free management snapshots;
- removal of Session `SyncRoot`, mutation locks and mutation leases.

Correctness, allocation, Release build, formatting, supply-chain and NuGet
consumer gates pass. The branch is nevertheless blocked by the ordinary release
gate: on the same machine and date, several MQTTZero development-matrix cells
retained less than 95 percent of the preceding baseline throughput.

Alternating same-machine A/B has now isolated the dominant regression to
`d14f433 refactor: move session mutations onto owning shards`. Every network
PUBLISH crossing to its Session Shard creates a new
`MqttZeroShardInvocation<T>`, `TaskCompletionSource<T>` and async execution
state, adding about 2.2 MiB per 1,000 QoS 0 publishes at 100 subscribers.
`d424810 -> d14f433` retains `0.862x` throughput for QoS 0 / 1 / 32 B and
`0.819x` for QoS 0 / 100 / 32 B over five alternating rounds.

The later `6eca005` de-lock commit is neutral, and direct five-round testing
did not confirm `ec8ba3f` as a throughput cause. Master-only q0/1/32-B drift is
small and not independently attributed to a single commit.

The follow-up hotpath repair restores QoS0/QoS1 throughput and per-message
allocation parity with `d424810`, including QoS1 / 100 / 64 KiB. It does not
pass the full gate: QoS0 tails, QoS0 / 100 / 8 KiB throughput and several QoS2
tails remain outside the five-percent limits.

The branch is not merged, no package version was raised and no performance
leadership claim is made. Evidence and raw results are recorded in
`perf/baseline/shard-owned-egress-timers-2026-11-26.md` and
`perf/baseline/shard-owned-egress-ab-investigation-2026-11-26.md`.

## Next Performance Step

The next step is to repair the identified cross-Shard command path:

1. Validate the remaining QoS0 and QoS2 tail cells with longer end-to-end
   measurement windows before changing their scheduling again.
2. Keep QoS1 and the pooled QoS0/PUBACK paths unchanged unless a repeatable
   exception is found.
3. Do not pipeline QoS2 PUBLISH unless the full PUBREC/PUBREL persistence and
   ordering suite remains green.
4. Re-run the debug matrix and strict p99 matrix before reopening the release
   gate.

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

The branch remains ineligible for integration. QoS0/QoS1 recovery is
significant, but the complete performance gate is still blocked.
