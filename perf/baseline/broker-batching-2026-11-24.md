# MQTTZero Outbound Batching Validation

## Changes

- Subscriber PUBLISH messages are drained in batches of up to 128 messages or 1 MiB and flushed once per batch.
- QoS 2 PUBREC/PUBREL/PUBCOMP packets are encoded directly into the connection PipeWriter.
- Messages without Message Expiry no longer read the current time in the routing and pump loops.
- Subscription Identifiers are allocated when subscriptions are created instead of once per delivery.
- Common queue-overflow cases evict from the head in O(1).

## Fast Single-Subscriber Matrix

Command:

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 0,1,2 --subscribers 1 --payload 32,8192,65536 `
  --warmup 2 --seconds 6 --repetitions 1
```

| QoS | Payload | MQTTZero / MQTTnet throughput | MQTTZero / MQTTnet p99 |
|---:|---:|---:|---:|
| 0 | 32 B | 1.77x | 0.27x |
| 0 | 8 KiB | 0.98x | 0.95x |
| 0 | 64 KiB | 1.10x | 3.57x |
| 1 | 32 B | 0.89x | 1.35x |
| 1 | 8 KiB | 1.00x | 1.00x |
| 1 | 64 KiB | 0.92x | 2.45x |
| 2 | 32 B | 1.06x | 0.94x |
| 2 | 8 KiB | 0.93x | 1.27x |
| 2 | 64 KiB | 0.96x | 1.00x |

After increasing the batch limit beyond 1 MiB, QoS 0 / 8 KiB reached approximately
`1.15x` throughput, but p99 slightly regressed above MQTTnet. QoS 0 / 64 KiB
reached approximately `1.30x`, with a substantially worse p99.

## Gate Status

The strict goal of `1.00x` throughput and non-worse p99 in every cell is not yet
met. The batch is retained because it materially improves QoS 0 medium messages,
but no `0.12.0-alpha.1` release or universal performance claim is made.

Remaining work is concentrated in QoS 1 acknowledgement latency, MQTT 5 property
transformation, unique-filter routing, and bounded output batching that improves
throughput without increasing p99.
