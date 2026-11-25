# Phase 0 Broker Development Baseline

## Environment

- Date: 2026-11-25
- OS: Microsoft Windows 10.0.26300
- CPU: Intel Core i7-8565U, 4 physical cores / 8 logical processors, 1.80 GHz base
- Runtime: .NET 10.0.15, win-x64
- MQTTnet: 5.2.0.1603
- Server GC: enabled
- Concurrent GC: enabled
- TieredPGO: enabled
- Minimum ThreadPool worker threads: 8
- Processor count: 8

## Command

```powershell
./eng/run-broker-benchmark.ps1 `
    -Client mqttnet `
    -OutputPath perf/baseline/broker-phase0-2026-11-25.json `
    -StartPort 30000
```

Development matrix:

- QoS: 0, 1
- Subscribers: 1, 100
- Payload: 32 B, 8 KiB, 64 KiB
- Warmup: 2 seconds
- Measurement: 6 seconds
- Repetitions: 1
- Closed-loop pending messages: 1,024

The 1,000-subscriber cells are intentionally deferred. The nightly stable
profile restores QoS 2 and uses 5-second warmup, 20-second measurement and
three repetitions.

## Gate

- Result: **FAILED**
- Cells checked: 12
- Failed operations in MQTTZero and MQTTnet: 0
- Passing cells: 3

This file records a valid development baseline, not a successful performance
claim.

| QoS | Subscribers | Payload | MQTTZero msg/s | MQTTnet msg/s | Throughput ratio | MQTTZero p99 us | MQTTnet p99 us | p99 ratio | Gate |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 0 | 1 | 32 B | 6,521.04 | 7,179.77 | 0.91x | 436.3 | 312.7 | 1.40x | FAIL |
| 0 | 1 | 8 KiB | 5,932.72 | 6,722.41 | 0.88x | 736.7 | 408.7 | 1.80x | FAIL |
| 0 | 1 | 64 KiB | 2,318.64 | 3,477.21 | 0.67x | 8,187.4 | 922.5 | 8.88x | FAIL |
| 0 | 100 | 32 B | 3,395.20 | 3,835.92 | 0.89x | 3,010.8 | 1,339.5 | 2.25x | FAIL |
| 0 | 100 | 8 KiB | 504.71 | 793.54 | 0.64x | 4,199.3 | 31,825.2 | 0.13x | FAIL |
| 0 | 100 | 64 KiB | 57.40 | 288.73 | 0.20x | 486,854.7 | 63,900.8 | 7.62x | FAIL |
| 1 | 1 | 32 B | 3,440.14 | 2,883.29 | 1.19x | 625.4 | 859.7 | 0.73x | PASS |
| 1 | 1 | 8 KiB | 3,254.78 | 2,579.70 | 1.26x | 684.0 | 1,003.1 | 0.68x | PASS |
| 1 | 1 | 64 KiB | 2,318.90 | 1,908.19 | 1.22x | 1,384.2 | 1,570.8 | 0.88x | PASS |
| 1 | 100 | 32 B | 269.81 | 263.30 | 1.02x | 9,859.5 | 9,083.6 | 1.09x | FAIL |
| 1 | 100 | 8 KiB | 162.44 | 186.77 | 0.87x | 18,940.3 | 15,065.7 | 1.26x | FAIL |
| 1 | 100 | 64 KiB | 48.10 | 52.89 | 0.91x | 85,292.6 | 46,934.9 | 1.82x | FAIL |

## Observations

- QoS 1 single-subscriber is the only currently green region.
- QoS 0 single-subscriber is slower in this short development profile, with
  the 64 KiB case showing the largest tail-latency gap.
- QoS 0, 100 subscribers and 64 KiB payload is the worst development cell:
  `0.20x` throughput and `7.62x` p99.
- All measured cells completed with zero failed operations, so the benchmark
  pipeline itself remained operational.
- Broker private memory includes short-lived process peak allocation and is
  not a steady-state per-connection capacity claim.

## Raw Client Diagnostic

Artifact: `artifacts/broker-phase0-raw-2026-11-25.json` (git-ignored local
diagnostic output).

```powershell
./eng/run-broker-benchmark.ps1 `
    -Client raw `
    -OutputPath artifacts/broker-phase0-raw-2026-11-25.json `
    -StartPort 32000
```

The raw client removes most MQTTnet Client overhead and shows a different
bottleneck shape:

| QoS | Subscribers | Payload | MQTTZero msg/s | MQTTnet msg/s | Throughput ratio | MQTTZero p99 us | MQTTnet p99 us | p99 ratio |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 B | 11,959.28 | 13,133.03 | 0.91x | 189.0 | 176.3 | 1.07x |
| 0 | 1 | 8 KiB | 6,596.00 | 7,139.51 | 0.92x | 269.7 | 279.3 | 0.97x |
| 0 | 1 | 64 KiB | 5,218.57 | 5,362.85 | 0.97x | 375.9 | 424.6 | 0.89x |
| 0 | 100 | 32 B | 8,943.89 | 3,222.87 | 2.78x | 303.0 | 465.5 | 0.65x |
| 0 | 100 | 8 KiB | 1,431.62 | 993.67 | 1.44x | 11,740.6 | 18,910.5 | 0.62x |
| 0 | 100 | 64 KiB | 280.73 | 421.91 | 0.67x | 74,576.4 | 58,568.0 | 1.27x |
| 1 | 1 | 32 B | 5,236.40 | 5,422.91 | 0.97x | 337.1 | 345.1 | 0.98x |
| 1 | 1 | 8 KiB | 4,043.67 | 3,964.17 | 1.02x | 469.5 | 604.4 | 0.78x |
| 1 | 1 | 64 KiB | 2,910.57 | 2,693.90 | 1.08x | 692.4 | 885.4 | 0.78x |
| 1 | 100 | 32 B | 283.93 | 279.58 | 1.02x | 7,395.2 | 9,443.3 | 0.78x |
| 1 | 100 | 8 KiB | 211.89 | 254.96 | 0.83x | 13,140.9 | 9,191.6 | 1.43x |
| 1 | 100 | 64 KiB | 143.81 | 141.37 | 1.02x | 17,100.0 | 18,278.4 | 0.94x |

The raw-client results confirm that the MQTTnet Client remains a material
component of several end-to-end results. They are diagnostic only and do not
replace the MQTTnet Client release gate.

## Linux

- Status: pending
- Expected workflow: `.github/workflows/performance.yml`
- Expected job: `broker-performance`
- Expected artifact: `broker-performance-ubuntu-latest`

No Linux result is claimed in this local report.
