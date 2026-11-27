# MQTTZero Shard-Owned Egress A/B Regression Investigation

## Decision

**Status: root cause identified; the branch remains blocked from release.**

The alternating same-machine A/B investigation separates the historical
comparison into two intervals:

- `baba03b -> 418aefc`: the master commits between the previous baseline and
  the feature branch point.
- `418aefc -> d573600`: the Session Egress, timer-wheel and Session de-lock
  feature branch.

The dominant reproducible throughput regression was introduced by
`d14f433 refactor: move session mutations onto owning shards`. It affects the
normal network PUBLISH path before the Session Egress stage:

| Cell | `d424810` msg/s | `d14f433` msg/s | Throughput | Delivery p99 | Broker CPU / 1K deliveries |
|---|---:|---:|---:|---:|---:|
| QoS 0 / 1 / 32 B | 32,143.75 | 27,692.49 | 0.862x | 1.414x | 2.133x |
| QoS 0 / 100 / 32 B | 477.82 | 391.08 | 0.819x | 5.063x | 2.939x |

Both are five-round alternating comparisons and all five paired rounds were
slower. QoS 0 / 100 / 32 B relative to `418aefc` reaches `0.661x` in the full
matrix. QoS 0 / 100 / 8 KiB reaches `0.925x`.

The later `6eca005 refactor: remove session synchronization locks` commit does
not add a repeatable throughput regression:

| Cell | `d14f433 -> 6eca005` throughput | Delivery p99 ratio | Slower rounds |
|---|---:|---:|---:|
| QoS 0 / 1 / 32 B | 1.013x | 0.963x | 1/5 |
| QoS 0 / 100 / 32 B | 0.996x | 0.996x | 2/5 |

The user hypothesis that `ec8ba3f` is the primary cause was not confirmed:

| Cell | `0c3637f -> ec8ba3f` throughput | Delivery p99 ratio | Slower rounds |
|---|---:|---:|---:|
| QoS 0 / 1 / 32 B | 0.984x | 0.139x | 3/5 |
| QoS 0 / 100 / 32 B | 1.048x | 1.106x | 1/5 |

`ec8ba3f` therefore does not explain the throughput regression. It may
contribute to the small QoS 0 fanout tail-latency drift, but that is secondary
and not the dominant failure.

## Environment And Method

| Item | Value |
|---|---|
| Date | 2026-11-26 |
| CPU | 12th Gen Intel Core i5-12450H, 8 cores / 12 logical processors |
| OS | Windows `10.0.26300`, x64 |
| Runtime | .NET `10.0.15` |
| SDK | .NET SDK `10.0.400` |
| Client | MQTTnet Client |
| Broker | Kestrel, Server GC, Concurrent GC and TieredPGO enabled |
| Shard topology | `EventLoopShardCount=0`, resolved to 12 |
| Kestrel IO queues | automatic, resolved to 12 |
| Egress batch | 512 KiB / 128 messages / 500 us |
| Window | 2 s warmup, 6 s measurement, 3 rounds, `max-pending=1024` |
| Additional rounds | Two rounds when either variant spread exceeded 10% or a boundary result needed resolution |

The three principal checkouts were extracted with `git archive`; no Git
worktree was created. Every commit was restored with `--locked-mode` and built
in Release. All A/B runs used the current HEAD Benchmark Driver, while the
Broker assembly came from the selected commit. Odd rounds ran baseline first
and even rounds ran candidate first.

## Full Development Matrix

Ratios are candidate divided by baseline medians. `5/5` means five rounds were
used after the spread rule; otherwise three rounds were used.

### `baba03b -> 418aefc`

| QoS | Subscribers | Payload | Rounds | Baseline msg/s | Candidate msg/s | Throughput | Candidate slower |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 B | 3 | 30,878.82 | 28,635.79 | 0.927x | 3/3 |
| 0 | 1 | 8 KiB | 5 | 2,683.26 | 2,710.28 | 1.010x | 2/5 |
| 0 | 100 | 32 B | 5 | 503.48 | 488.06 | 0.970x | 3/5 |
| 0 | 100 | 8 KiB | 3 | 154.69 | 157.35 | 1.017x | 1/3 |
| 1 | 1 | 32 B | 3 | 1,778.16 | 1,741.29 | 0.979x | 2/3 |
| 1 | 1 | 8 KiB | 3 | 1,518.47 | 1,500.25 | 0.988x | 2/3 |
| 1 | 100 | 32 B | 3 | 93.46 | 94.52 | 1.011x | 0/3 |
| 1 | 100 | 8 KiB | 3 | 70.24 | 67.68 | 0.964x | 2/3 |
| 2 | 1 | 32 B | 3 | 932.63 | 914.93 | 0.981x | 2/3 |
| 2 | 1 | 8 KiB | 5 | 808.64 | 805.29 | 0.996x | 3/5 |
| 2 | 100 | 32 B | 3 | 41.12 | 43.46 | 1.057x | 1/3 |
| 2 | 100 | 8 KiB | 3 | 37.40 | 38.66 | 1.034x | 0/3 |

The only row beyond the five-percent threshold is QoS 0 / 1 / 32 B. Master
commit-level bisection found no direct adjacent comparison that independently
confirmed a greater-than-five-percent loss:

| Comparison | Rounds | Throughput | Notes |
|---|---:|---:|---|
| `baba03b -> 4f8e8ec` | 3 | 0.997x | Not regressed |
| `baba03b -> e27d688` | 3 | 0.967x | Not regressed |
| `baba03b -> cf71138` | 3 | 0.947x | Boundary result |
| `e27d688 -> cf71138` | 5 | 0.961x | Direct comparison did not confirm |
| `0c3637f -> ec8ba3f` | 5 | 0.984x | Direct comparison did not confirm |

This remains a small, non-attributed master/environment drift rather than the
dominant branch regression.

### `418aefc -> d573600`

| QoS | Subscribers | Payload | Rounds | Baseline msg/s | Candidate msg/s | Throughput | Candidate slower | p99 ratio |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 B | 3 | 31,316.62 | 27,072.98 | 0.864x | 3/3 | 0.159x |
| 0 | 1 | 8 KiB | 5 | 2,972.32 | 2,947.04 | 0.992x | 2/5 | 1.203x |
| 0 | 100 | 32 B | 3 | 571.08 | 377.71 | 0.661x | 3/3 | 5.283x |
| 0 | 100 | 8 KiB | 5 | 133.94 | 123.90 | 0.925x | 4/5 | 1.086x |
| 1 | 1 | 32 B | 3 | 1,630.08 | 1,706.08 | 1.047x | 0/3 | 0.990x |
| 1 | 1 | 8 KiB | 3 | 1,496.07 | 1,556.14 | 1.040x | 1/3 | 0.980x |
| 1 | 100 | 32 B | 3 | 94.78 | 93.12 | 0.982x | 2/3 | 1.400x |
| 1 | 100 | 8 KiB | 3 | 68.91 | 68.81 | 0.999x | 1/3 | 1.219x |
| 2 | 1 | 32 B | 3 | 890.09 | 908.24 | 1.020x | 1/3 | 0.934x |
| 2 | 1 | 8 KiB | 5 | 810.70 | 806.18 | 0.994x | 2/5 | 1.104x |
| 2 | 100 | 32 B | 5 | 47.33 | 46.71 | 0.987x | 3/5 | 1.206x |
| 2 | 100 | 8 KiB | 3 | 38.10 | 39.58 | 1.039x | 1/3 | 1.034x |

Confirmed throughput regressions are limited to QoS 0:

- QoS 0 / 1 / 32 B: `-13.6%`, CPU per delivered message `2.28x`.
- QoS 0 / 100 / 32 B: `-33.9%`, CPU per delivered message `2.17x`.
- QoS 0 / 100 / 8 KiB: `-7.5%`, CPU per delivered message approximately neutral.

QoS 1 fanout throughput is neutral in the full matrix, but delivery p99 is
worse: QoS 1 / 100 / 32 B `1.40x`, QoS 1 / 100 / 8 KiB `1.22x`.

## 64 KiB Safety Cells

All four cells needed five rounds because the observed spread exceeded ten
percent.

| QoS | Subscribers | Baseline msg/s | Candidate msg/s | Throughput | Baseline p99 us | Candidate p99 us | p99 ratio |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 1,053.74 | 819.31 | 0.778x | 2,080,686 | 1,999,890 | 0.961x |
| 0 | 100 | 23.39 | 24.08 | 1.030x | 1,960,677 | 2,343,190 | 1.195x |
| 1 | 1 | 752.01 | 809.85 | 1.077x | 313,246 | 576,318 | 1.840x |
| 1 | 100 | 20.58 | 19.59 | 0.952x | 61,141 | 104,451 | 1.708x |

Large-payload behavior is mixed: QoS 0 single-subscriber throughput also
regresses, while QoS 1 single-subscriber improves throughput. Tail latency is
not stable in these cells, and QoS 1 / 100 has five-of-five p99 regressions.

## Stage Diagnostics

`outbound --diagnostics true`, QoS 0 / 32 B, 3 s warmup and 10 s measurement:

| Subscribers | Variant | End-to-end msg/s | Avg fanout us | Max fanout us | Scheduler us | Drain us |
|---:|---|---:|---:|---:|---:|---:|
| 1 | `d424810` | 65,549.53 | 5.56 | 3,474.4 | 3.46 | 3.05 |
| 1 | `d14f433` | 61,847.01 | 5.82 | 14,838.3 | 3.66 | 3.25 |
| 100 | `d424810` | 440.05 | 81.02 | 3,748.5 | 19.27 | 2.05 |
| 100 | `d14f433` | 410.99 | 105.10 | 13,551.3 | 21.90 | 2.33 |

The Egress scheduler and drain stages are nearly flat. Fanout work increases,
and its maximum latency rises by roughly four times. This places the regression
before Session Egress rather than in encoding, batching, PipeWriter writes or
Flush.

## Allocation Evidence

`memory` mode, QoS 0 / 100 / 32 B, 1,000 publishes per wave:

| Commit | Wave 2 allocation delta | Wave 3 allocation delta | Gen0 at wave 3 |
|---|---:|---:|---:|
| `d424810` | 420,168 B | 330,392 B | 8 |
| `d14f433` | 2,282,464 B | 2,207,440 B | 8 |

The new per-wave allocation is approximately 2.2 MiB, about five to seven times
the prior incremental allocation.

## Root Cause

`d14f433` moves every owned PUBLISH from the connection read loop onto the
Session Shard:

```csharp
action = await ShardSet[context.Session.ShardId].InvokeAsync(
    context.CachedOwnedPublishInvocation,
    context.ConnectionLifetime.Token);
```

When the read loop is not already executing on the target Session Shard,
`MqttZeroEventLoop.InvokeAsync` creates a new `MqttZeroShardInvocation<T>`.
That object contains a new `TaskCompletionSource<T>` configured with
`RunContinuationsAsynchronously`, followed by an async `ExecuteAsync` state
machine and cross-shard wake/wait. This happens on the per-PUBLISH hot path.

The same commit changes the hot-path allocation test to execute the measured
loop inside the target Shard. That test therefore measures same-shard inline
invocation and misses the real connection-reader-to-Session-Shard path that
allocates and waits. This is why correctness and the existing allocation
sentinel passed while end-to-end throughput regressed.

The dominant issue is not:

- the per-Shard timer wheel;
- the Session Egress state machine;
- Flush or encoding;
- Session lock removal in `6eca005`;
- `ec8ba3f` and its `AcceptConnectionCoreAsync` refactor.

## Raw Results

The investigation outputs are stored under:

`artifacts/broker-ab/shard-owned-regression-2026-11-26/`

The committed evidence set is stored under:

`perf/baseline/shard-owned-egress-ab-raw-2026-11-26/`

## Next Action

The follow-up implementation replaced the QoS0/QoS1 hot path with direct
acks and pooled shard work. Its result is recorded in
`perf/baseline/shard-command-hotpath-2026-11-27.md`.

QoS0/QoS1 allocation and throughput recover, while QoS0 tails and QoS2 tails
remain blocked. Any further change must preserve:

- Session Shard ownership;
- ordering per publishing connection;
- cancellation and shutdown;
- zero allocation after warmup;
- exact-once command ownership and exception propagation.

The first failing test must exercise an actual off-Shard PUBLISH and measure
the full enqueue plus acknowledgment path, not only queue mutation inside the
target Shard.
