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

## Write-lock-free control revision

- `MqttZeroClientConnection` no longer owns a `SemaphoreSlim` write lock or
  per-connection fixed writer array.
- Attached connections pre-encode every control packet into a pooled
  `MqttZeroEgressControlFrame`; the Egress Actor is the only active control
  writer.
- Control-frame completion uses `IValueTaskSource`, so 1,000 PubAck operations
  remain `0 B` after warmup.
- Direct control encoding remains only for pre-attach handshake and the final
  DISCONNECT after Egress has stopped.
- Actor shutdown waits for an in-flight Flush before exposing `Stopped`, which
  prevents a final control packet from racing the previous network Flush.

Reduced 3 x 6 second diagnostic for QoS 0 / 100 subscribers / 8 KiB:

| Revision | MQTTZero msg/s | p99 us | Result |
|---|---:|---:|---|
| Phase 0 reference | 504.71 | 4,199.3 | baseline |
| current write-lock-free | 381.70–445.17 | 50,030.7–110,310.0 | FAIL |

An EventPipe allocation trace under the same workload attributed 97.75% of
sampled allocation bytes to `PipeWriter.GetSpan` through
`MqttPublishPacketCodec.EncodeToWriter`. The codec now calls `GetSpan()` without
a size hint and consumes only the segment chosen by Kestrel, with a regression
test that rejects positive size hints on the outbound data path. This respects
Kestrel's pooled segment policy but does not yet pass the performance gate.

MQTTnet.AspNetCore also uses the Kestrel `PipeWriter`; bypassing Kestrel is not
the conclusion. The remaining differences to investigate are connection-local
send-loop lifetime, flush cadence, packet-bus wakeups and Shard rescheduling
under 100-connection fanout. The current result does not justify replacing the
transport.

