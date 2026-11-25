# Phase 1 Event Loop Shard Development Baseline

## Environment

- Date: 2026-11-25
- OS: Microsoft Windows 10.0.26300
- Runtime: .NET 10.0.15, win-x64
- ProcessorCount: 8
- EventLoopShardCount: `0`, resolved to 8 logical processors
- Kestrel IOQueueCount: 8
- Server GC, Concurrent GC and TieredPGO: enabled
- Minimum ThreadPool worker threads: 8

## Command

```powershell
./eng/run-broker-benchmark.ps1 `
    -Client mqttnet `
    -OutputPath perf/baseline/broker-phase1-2026-11-25.json `
    -StartPort 30000
```

Development matrix:

- QoS: 0, 1
- Subscribers: 1, 100
- Payload: 32 B, 8 KiB, 64 KiB
- Warmup: 2 seconds
- Measurement: 6 seconds
- Repetitions: 1

## Regression Gate

Compared with `broker-phase0-2026-11-25.json`:

- Gate result: **FAILED**
- Limit: throughput at least 95% of Phase 0; p99 no more than 105% of Phase 0
- Failed operations: 0
- Passing cells: 7 of 12

| QoS | Subscribers | Payload | Phase 0 msg/s | Phase 1 msg/s | Throughput ratio | Phase 0 p99 us | Phase 1 p99 us | p99 ratio | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | 1 | 32 B | 6,521.04 | 6,804.13 | 1.04x | 436.3 | 364.7 | 0.84x | PASS |
| 0 | 1 | 8 KiB | 5,932.72 | 6,066.40 | 1.02x | 736.7 | 499.3 | 0.68x | PASS |
| 0 | 1 | 64 KiB | 2,318.64 | 2,337.45 | 1.01x | 8,187.4 | 8,788.7 | 1.07x | FAIL |
| 0 | 100 | 32 B | 3,395.20 | 4,653.60 | 1.37x | 3,010.8 | 1,363.8 | 0.45x | PASS |
| 0 | 100 | 8 KiB | 504.71 | 359.76 | 0.71x | 4,199.3 | 67,254.5 | 16.02x | FAIL |
| 0 | 100 | 64 KiB | 57.40 | 53.41 | 0.93x | 486,854.7 | 307,200.0 | 0.63x | FAIL |
| 1 | 1 | 32 B | 3,440.14 | 3,929.83 | 1.14x | 625.4 | 522.6 | 0.84x | PASS |
| 1 | 1 | 8 KiB | 3,254.78 | 3,604.42 | 1.11x | 684.0 | 527.2 | 0.77x | PASS |
| 1 | 1 | 64 KiB | 2,318.90 | 2,706.27 | 1.17x | 1,384.2 | 1,076.7 | 0.78x | PASS |
| 1 | 100 | 32 B | 269.81 | 275.80 | 1.02x | 9,859.5 | 8,300.0 | 0.84x | PASS |
| 1 | 100 | 8 KiB | 162.44 | 180.16 | 1.11x | 18,940.3 | 26,061.4 | 1.38x | FAIL |
| 1 | 100 | 64 KiB | 48.10 | 44.36 | 0.92x | 85,292.6 | 94,844.4 | 1.11x | FAIL |

## Interpretation

- Single-subscriber QoS 1 and small/medium QoS 0 improved.
- The largest anomaly is QoS 0, 100 subscribers, 8 KiB: throughput fell to
  0.71x and p99 increased 16.02x.
- The shard scheduler is not on the synchronous QoS 0 fanout path yet, so this
  result is not accepted as evidence that Shard scheduling caused all
  regressions. It may include host noise and current Route/Delivery behavior.
- Phase 1 is functionally complete but does not pass the development
  performance gate. This branch must not claim a performance improvement.

## Runtime Observations

| QoS | Subscribers | Payload | Broker CPU ms | Broker private MiB | Load CPU ms | Load allocations MiB |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 B | 18,343.8 | 24.9 | 11,656.2 | 92.5 |
| 0 | 1 | 8 KiB | 16,890.6 | 52.2 | 11,484.4 | 932.6 |
| 0 | 1 | 64 KiB | 9,875.0 | 635.2 | 7,234.4 | 1,639.2 |
| 0 | 100 | 32 B | 22,890.6 | 183.4 | 17,687.5 | 3,396.4 |
| 0 | 100 | 8 KiB | 11,859.4 | 582.0 | 28,296.9 | 3,677.4 |
| 0 | 100 | 64 KiB | 7,890.6 | 324.6 | 28,187.5 | 3,910.9 |
| 1 | 1 | 32 B | 14,656.2 | 37.5 | 8,156.2 | 94.3 |
| 1 | 1 | 8 KiB | 16,078.1 | 25.3 | 9,343.8 | 595.1 |
| 1 | 1 | 64 KiB | 11,296.9 | 445.1 | 12,390.6 | 0.0 |
| 1 | 100 | 32 B | 26,296.9 | 206.2 | 20,515.6 | 295.0 |
| 1 | 100 | 8 KiB | 25,937.5 | 202.5 | 18,046.9 | 1,902.1 |
| 1 | 100 | 64 KiB | 11,500.0 | 224.0 | 24,734.4 | 3,415.8 |

## Implemented Shard Behavior

- `MqttZeroInboundDispatcher` has been removed.
- Event loop shards use a bounded queue and one serialized drain.
- Drain budget is 1,024 work items or 500 microseconds.
- Session Shard is derived from an FNV-1a hash of UTF-8 Client ID.
- Short Client ID buffers use `stackalloc`; longer buffers use `ArrayPool<byte>`.
- CONNECT executes on the target Session Shard.
- Connection-scoped management calls execute through the owning Session Shard.
- Kestrel `IOQueueCount` defaults to the resolved Shard count and can be
  overridden by a later `SocketTransportOptions` configuration.

## Linux

- Status: pending
- Workflow: `.github/workflows/performance.yml`
- Job: `broker-performance`
- Artifact: `broker-performance-ubuntu-latest`

