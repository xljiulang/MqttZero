# Phase 2 Nonblocking Flush Baseline

- Environment: Windows, .NET 10, 8 logical processors
- Matrix: QoS 0/1, subscribers 1/100, payload 32 B/8 KiB/64 KiB
- Result: **FAILED**, 4 of 12 cells outside Phase 0 tolerance
- Failed operations: 0

| QoS | Subscribers | Payload | Throughput ratio | p99 ratio | Result |
|---:|---:|---:|---:|---:|---|
| 0 | 1 | 32 B | 1.11x | 0.66x | PASS |
| 0 | 1 | 8 KiB | 1.13x | 0.74x | PASS |
| 0 | 1 | 64 KiB | 1.06x | 0.91x | PASS |
| 0 | 100 | 32 B | 1.44x | 0.37x | PASS |
| 0 | 100 | 8 KiB | 0.85x | 20.63x | FAIL |
| 0 | 100 | 64 KiB | 1.09x | 0.52x | PASS |
| 1 | 1 | 32 B | 1.14x | 1.00x | PASS |
| 1 | 1 | 8 KiB | 0.80x | 1.99x | FAIL |
| 1 | 1 | 64 KiB | 0.60x | 2.99x | FAIL |
| 1 | 100 | 32 B | 1.02x | 0.97x | PASS |
| 1 | 100 | 8 KiB | 0.94x | 1.18x | FAIL |
| 1 | 100 | 64 KiB | 1.05x | 0.56x | PASS |

The nonblocking Flush path removed the previous QoS 0 large-payload and
100-subscriber/64-KiB regressions. Remaining failures are concentrated in
QoS 0/100/8 KiB and QoS 1 mid/large payload paths.

