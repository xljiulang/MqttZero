# MQTTZero Final Single-Subscriber Gate

## Environment

- Windows 11, 12th Gen Intel Core i5-12450H
- .NET SDK 10.0.400, Release
- MQTTnet 5.2.0.1603
- Three repetitions per cell, median values
- Command:

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 0,1,2 --subscribers 1 --payload 32,8192,65536 `
  --warmup 2 --seconds 6 --repetitions 3
```

## Results

| QoS | Payload | Throughput ratio | p99 ratio |
|---:|---:|---:|---:|
| 0 | 32 B | 1.94x | 0.21x |
| 0 | 8 KiB | 1.16x | 1.01x |
| 0 | 64 KiB | 1.63x | 1.17x |
| 1 | 32 B | 0.98x | 1.18x |
| 1 | 8 KiB | 1.01x | 0.97x |
| 1 | 64 KiB | 0.95x | 1.48x |
| 2 | 32 B | 1.01x | 1.01x |
| 2 | 8 KiB | 0.97x | 1.05x |
| 2 | 64 KiB | 0.99x | 0.95x |

## Gate Status

The strict gate requires throughput of at least `1.00x` and p99 no worse than
MQTTnet in every cell. The gate is not met.

QoS 0 passes throughput comfortably. QoS 1/2 are approximately at parity, but
do not meet the required lead. QoS 1/64 KiB remains the largest remaining gap.
No `0.12.0-alpha.1` version bump, merge, or push is performed.

## Key Fanout Matrix

Command:

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 0,1 --subscribers 100,1000 --payload 8192,65536 `
  --warmup 2 --seconds 6 --repetitions 3
```

| QoS | Subscribers | Payload | Throughput ratio | p99 ratio |
|---:|---:|---:|---:|---:|
| 0 | 100 | 8 KiB | 1.13x | 0.93x |
| 0 | 100 | 64 KiB | 1.03x | 1.75x |
| 0 | 1000 | 8 KiB | 1.09x | 0.65x |
| 0 | 1000 | 64 KiB | 0.94x | 2.63x |
| 1 | 100 | 8 KiB | 1.10x | 0.77x |
| 1 | 100 | 64 KiB | 1.04x | 1.03x |
| 1 | 1000 | 8 KiB | 1.06x | 0.91x |
| 1 | 1000 | 64 KiB | 0.55x | 5.22x |

The 1000-subscriber / 64 KiB cases remain the largest scalability gaps. They
must not be treated as capacity claims for the current alpha.

## Correctness Review Fixes

- Inbound Work now owns an independent Lease reference.
- BufferPool capacity wakeups were restored to the non-lossy unconditional
  semaphore release path.
- Topic Trie now distinguishes absent levels from trailing empty levels.
- Binary MQTT property lengths are parsed as unsigned values.
- InboundDispatch restore processes messages in Client ID + MessageId order.
- Shared-subscription cursors survive route snapshot rebuilds.
- Message queue ownership now uses an explicit queued flag.
- Large message batches use four-message batches only when a subscriber queue
  already has backlog; isolated messages continue to flush individually.

These fixes are correctness and operational-safety changes. They do not change
the strict performance gate result.
