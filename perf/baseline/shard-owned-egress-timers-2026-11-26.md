# MQTTZero Shard-Owned Egress And Timer Baseline

## Decision

**Status: BLOCKED by the same-machine regression gate.**

The shard-owned Session Egress, per-shard timer wheel and Session-lock removal
pass correctness, allocation and supply-chain gates. The development comparison
contains zero failed operations, zero missing deliveries and complete drain in
every cell.

The branch does not pass the required same-machine MQTTZero regression gate.
Several cells are below the preceding `master-hardening-rebaseline` result by
more than five percent, and the MQTTnet control also drifted on this notebook.
The comparison therefore cannot be used to claim a stable release baseline.

Consequences:

- Do not merge `refactor/shard-owned-egress-timers` to `master`.
- Do not raise the package version.
- Keep the branch for performance investigation.
- Do not claim general superiority over MQTTnet.

## Environment

| Item | Value |
|---|---|
| Date | 2026-11-26 |
| CPU | 12th Gen Intel Core i5-12450H, 8 cores / 12 logical processors |
| OS | Windows 11 专业版 10.0.26300 |
| Runtime | .NET 10.0.15 |
| SDK | .NET SDK 10.0.400 |
| Broker topology | `EventLoopShardCount=0`, resolved to 12 |
| Kestrel IO queues | `IOQueueCount=0`, resolved to 12 |
| Egress batch | 512 KiB / 128 messages / 500 us |
| Branch | `refactor/shard-owned-egress-timers` |
| Verified commit | `6eca005` |
| Comparison client | MQTTnet Client |

Both MQTTZero and MQTTnet were hosted by Kestrel. Each Broker ran in an isolated
subprocess. Server GC, concurrent GC and TieredPGO were enabled.

## Commands

Development matrix, run once per QoS:

```powershell
dotnet benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet --qos <0|1|2> --subscribers 1,100 --payload 32,8192 `
  --warmup 2 --seconds 6 --repetitions 2 --max-pending 1024 `
  --start-port <unique-range> --comparison-gate false `
  --output <perf/baseline/json>
```

64 KiB safety cells:

```powershell
dotnet benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet --qos <0|1> --subscribers 1,100 --payload 65536 `
  --warmup 2 --seconds 6 --repetitions 2 --max-pending 1024 `
  --start-port <unique-range> --comparison-gate false `
  --output <perf/baseline/json>
```

The harness automatically added a third repetition to cells whose two-run
throughput spread exceeded ten percent.

## Correctness And Allocation Gates

| Gate | Result |
|---|---|
| Locked restore | PASS |
| Release build | PASS, 0 warnings / 0 errors |
| Core tests | PASS, 754/754 |
| Dashboard .NET tests | PASS, 24/24 |
| Broker benchmark tests | PASS, 71/71 |
| Allocation sentinels | PASS, 22/22 |
| Format verification | PASS |
| Session lock/Source guard | PASS, zero production references |
| Vulnerable packages | PASS, none |
| Deprecated packages | PASS, none |
| NuGet pack | PASS, all three packages |
| Repository-external consumers | PASS, 0 warnings / 0 errors |

## Development Matrix Medians

Ratios below are MQTTZero divided by MQTTnet for end-to-end throughput. The
structural comparison gate passed with ranking enforcement disabled.

| QoS | Subscribers | Payload | MQTTZero msg/s | MQTTnet msg/s | Throughput ratio | MQTTZero p99 us | MQTTnet p99 us |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 B | 27,032.14 | 30,866.27 | 0.876 | 231 | 206 |
| 0 | 1 | 8 KiB | 2,746.51 | 1,909.55 | 1.438 | 597 | 493 |
| 0 | 100 | 32 B | 335.69 | 338.67 | 0.991 | 142 | 137 |
| 0 | 100 | 8 KiB | 112.12 | 107.97 | 1.038 | 28,164 | 16,532 |
| 1 | 1 | 32 B | 1,721.94 | 1,662.83 | 1.036 | 1,044 | 1,140 |
| 1 | 1 | 8 KiB | 1,285.76 | 1,321.12 | 0.973 | 1,583 | 1,285 |
| 1 | 100 | 32 B | 87.30 | 86.23 | 1.012 | 16,161 | 16,200 |
| 1 | 100 | 8 KiB | 59.27 | 64.33 | 0.921 | 28,127 | 22,118 |
| 2 | 1 | 32 B | 756.21 | 783.68 | 0.965 | 2,199 | 1,990 |
| 2 | 1 | 8 KiB | 706.00 | 718.78 | 0.982 | 2,294 | 2,258 |
| 2 | 100 | 32 B | 41.65 | 42.40 | 0.982 | 29,608 | 30,203 |
| 2 | 100 | 8 KiB | 34.87 | 35.92 | 0.971 | 39,663 | 34,982 |

## 64 KiB Safety Cells

| QoS | Subscribers | Payload | MQTTZero msg/s | MQTTnet msg/s | Throughput ratio | MQTTZero p99 us | MQTTnet p99 us |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 64 KiB | 638.10 | 738.24 | 0.864 | 5,769 | 4,254 |
| 0 | 100 | 64 KiB | 21.46 | 24.03 | 0.893 | 45,643 | 51,406 |
| 1 | 1 | 64 KiB | 591.98 | 782.05 | 0.757 | 12,848 | 8,927 |
| 1 | 100 | 64 KiB | 17.50 | 17.79 | 0.984 | 101,835 | 96,244 |

All cells reported zero failed operations and complete delivery.

## Same-Machine Regression Against Previous Baseline

Compared with `master-hardening-rebaseline-qos<0|1|2>-2026-11-26.json`, raw
MQTTZero throughput retention is below the five-percent gate in all development
matrix cells:

| QoS | Subscribers | Payload | Previous MQTTZero msg/s | Current MQTTZero msg/s | Retention |
|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 B | 31,842.17 | 27,032.14 | 0.849 |
| 0 | 1 | 8 KiB | 3,029.53 | 2,746.51 | 0.907 |
| 0 | 100 | 32 B | 418.81 | 335.69 | 0.802 |
| 0 | 100 | 8 KiB | 178.14 | 112.12 | 0.629 |
| 1 | 1 | 32 B | 2,083.32 | 1,721.94 | 0.827 |
| 1 | 1 | 8 KiB | 1,674.16 | 1,285.76 | 0.768 |
| 1 | 100 | 32 B | 110.24 | 87.30 | 0.792 |
| 1 | 100 | 8 KiB | 73.79 | 59.27 | 0.803 |
| 2 | 1 | 32 B | 926.40 | 756.21 | 0.816 |
| 2 | 1 | 8 KiB | 841.83 | 706.00 | 0.839 |
| 2 | 100 | 32 B | 55.26 | 41.65 | 0.754 |
| 2 | 100 | 8 KiB | 47.01 | 34.87 | 0.742 |

The MQTTnet control also changed on the current notebook. After normalizing by
the current and previous MQTTnet ratios, QoS 0 fanout still retained only about
`0.76-0.78x`; several QoS 1/2 cells retained `0.90-0.97x`. This is sufficient
evidence to block release, even though the drift prevents attributing the full
raw delta to one code change.

## Architecture Diagnostics

- One `MqttZeroSessionEgress` owns control ordering, Flush, Pending/Inflight
  selection, encoding handoff and QoS 0 release.
- Every Shard owns one four-level intrusive timer wheel with 512 slots per level.
- The global deadline queue, global maintenance scan and `PeriodicTimer` loop are
  removed.
- Session `SyncRoot`, `SessionMutationLock`, `SessionMutationLease` and
  `AcquireSessionMutationAsync` are removed.
- Management and Route Epoch reads use atomically published lifecycle,
  subscription and counter snapshots.
- No rejected optimization was retained. Prior universal Writer, forced
  fairness, backlog and Socket experiments remain deleted.

## Raw Results

- `perf/baseline/shard-owned-egress-timers-qos0-2026-11-26.json`
- `perf/baseline/shard-owned-egress-timers-qos0-64k-2026-11-26.json`
- `perf/baseline/shard-owned-egress-timers-qos1-2026-11-26.json`
- `perf/baseline/shard-owned-egress-timers-qos1-64k-2026-11-26.json`
- `perf/baseline/shard-owned-egress-timers-qos2-2026-11-26.json`

## Next Action

Investigate the measured regression before merging:

1. Compare current and previous commits with the same Broker and client
   processes under alternating A/B runs to separate thermal and machine drift.
2. Instrument Shard enqueue age, timer-wheel tick duration, fanout dispatch and
   Session Egress batch execution in the regressed QoS 0/1 fanout cells.
3. Re-run the 1/100 subscriber 32 B and 8 KiB matrix before attempting any
   further architectural change.
