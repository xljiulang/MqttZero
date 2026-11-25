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

## Official stability run

Command:

```powershell
dotnet run -c Release --no-build `
    --project benchmarks/MqttZero.BrokerBenchmarks -- `
    compare --client mqttnet --qos 0,1 --subscribers 100 --payload 8192 `
    --warmup 5 --seconds 20 --repetitions 3 --max-pending 1024 `
    --start-port 36000 `
    --output perf/baseline/broker-phase2-official-2026-11-25.json
```

| QoS | MQTTZero msg/s | MQTTZero p99 us | MQTTNet msg/s | MQTTNet p99 us | Phase 0 result |
|---:|---:|---:|---:|---:|---|
| 0 | 492.88 | 36,631.1 | 1,226.38 | 20,105.5 | throughput within 95%, p99 regression |
| 1 | 190.72 | 11,808.6 | 192.79 | 15,939.7 | throughput and p99 pass |

QoS 0 throughput is 97.6% of the Phase 0 MQTTZero baseline, but its p99 is
8.72x the Phase 0 value. This is the only remaining Phase 2 stability failure.
QoS 1 is effectively at MQTTnet parity and comfortably passes the Phase 0
regression tolerance.

## Matched runtime counters

Both brokers were sampled for eight seconds under the same QoS 0, 100
subscriber and 8 KiB workload.

| Counter | MQTTZero | MQTTNet |
|---|---:|---:|
| Allocation rate mean | 5.98 MB/s | 190.96 MB/s |
| GC pause mean | 4.39 ms/s | 81.12 ms/s |
| CPU usage mean | 9.65% | 24.51% |
| ThreadPool completed work items mean | 2,384/s | 42,746/s |
| ThreadPool queue length mean | 2.65 | 33.81 |

MQTTZero now allocates and pauses significantly less than MQTTnet. The remaining
gap is therefore not GC, the large-span request pattern, Shard count or a
per-connection long-running send loop. The latter prototype measured about
304 msg/s and p99 97 ms and was removed.

The repository's in-memory 100-subscriber route benchmark is approximately
16.78 us per publish, or about 59.6k deliveries/s. The current official result
performs roughly 49.3k deliveries/s, already close to that ceiling. This points
to the current one-candidate-at-a-time Route/Delivery fanout as the dominant
remaining limit.

## Phase decision

- Keep the Phase 2 Shard Egress, nonblocking flush and pooled control frames.
- Do not change the Kestrel IOQueue default: a one-queue experiment improved
  QoS 0 but reduced QoS 1/100/8 KiB from about 191 to 86 msg/s.
- Do not continue Phase 2 queue, GC or send-loop tuning.
- Record QoS 0/100/8 KiB p99 as the Phase 2 performance exception and move to
  Phase 4 aggregated fanout batches by target Shard.

