# Phase 2 Egress Actor Development Baseline

## Environment

- Windows 11, .NET 10.0.15, X64
- ProcessorCount: 8
- EventLoopShardCount: 8
- MQTTnet 5.2.0.1603
- Development matrix: QoS 0/1, subscribers 1/100, payload 32 B/8 KiB/64 KiB
- Warmup 2 s, measurement 6 s, one repetition

## Command

```powershell
./eng/run-broker-benchmark.ps1 `
    -Client mqttnet `
    -OutputPath perf/baseline/broker-phase2-2026-11-25.json `
    -StartPort 30000
```

## Phase 0 Regression Gate

- Result: **FAILED**
- Failed operations: 0

| QoS | Subscribers | Payload | Phase 0 msg/s | Phase 2 msg/s | Throughput ratio | Phase 0 p99 us | Phase 2 p99 us | p99 ratio | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | 1 | 32 B | 6,521.04 | 7,365.71 | 1.13x | 436.3 | 282.8 | 0.65x | PASS |
| 0 | 1 | 8 KiB | 5,932.72 | 6,512.85 | 1.10x | 736.7 | 425.7 | 0.58x | PASS |
| 0 | 1 | 64 KiB | 2,318.64 | 2,118.12 | 0.91x | 8,187.4 | 8,859.0 | 1.08x | FAIL |
| 0 | 100 | 32 B | 3,395.20 | 3,137.96 | 0.92x | 3,010.8 | 784.3 | 0.26x | FAIL |
| 0 | 100 | 8 KiB | 504.71 | 310.95 | 0.62x | 4,199.3 | 86,891.9 | 20.69x | FAIL |
| 0 | 100 | 64 KiB | 57.40 | 323.47 | 5.63x | 486,854.7 | 68,153.7 | 0.14x | PASS |
| 1 | 1 | 32 B | 3,440.14 | 4,162.12 | 1.21x | 625.4 | 519.2 | 0.83x | PASS |
| 1 | 1 | 8 KiB | 3,254.78 | 3,180.41 | 0.98x | 684.0 | 785.7 | 1.15x | FAIL |
| 1 | 1 | 64 KiB | 2,318.90 | 2,668.06 | 1.15x | 1,384.2 | 1,280.2 | 0.93x | PASS |
| 1 | 100 | 32 B | 269.81 | 287.86 | 1.07x | 9,859.5 | 7,790.0 | 0.79x | PASS |
| 1 | 100 | 8 KiB | 162.44 | 162.68 | 1.00x | 18,940.3 | 16,861.8 | 0.89x | PASS |
| 1 | 100 | 64 KiB | 48.10 | 41.12 | 0.85x | 85,292.6 | 135,390.9 | 1.59x | FAIL |

## Interpretation

- The largest Phase 1 regression, QoS 0 / 100 / 64 KiB, improved from
  0.93x Phase 0 throughput to 5.63x.
- QoS 0 / 100 / 8 KiB remains the dominant unresolved anomaly.
- QoS 1 / 100 / 64 KiB still regresses in throughput and p99.
- The Phase 2 regression gate is red in 5 of 12 cells. No universal
  performance claim is made.

## Implementation Status

- Data delivery now uses `MqttZeroEgressActor`.
- `MqttZeroSessionOutboundPump` and `MqttZeroConnectionWriter` are deleted.
- Egress Signal coalescing, control-before-data ordering and nonblocking
  FlushPending are covered by unit tests.
- Main control Send APIs now forward through the Egress control queue when a
  connection is attached. The low-level write lock is retained for direct
  connection tests and a small number of direct paths.
