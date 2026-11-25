# Phase 4.5 Egress Hot-Path Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining Phase 4 network hot-path regressions before starting Phase 5.

**Architecture:** Add repeatable topology and per-stage diagnostics first. Use the evidence to change either QoS 0 fanout pipelining, Egress batching/Flush behavior, or large-Payload transport behavior; do not modify Route Trie or Planner unless a new benchmark disproves the existing zero-allocation microbenchmark result.

**Tech Stack:** .NET 10, Kestrel PipeWriter, MQTT 5, Event Loop Shards, MQTTnet 5.2.0.1603 comparison client, xUnit v3, BenchmarkDotNet, PowerShell 7.

**Spec:** `docs/sharded-actor-broker-plan.md` Phases 2 and 4; `perf/baseline/broker-phase4-2026-11-25.md`.

## Global Constraints

- Target framework is `net10.0`.
- MQTTZero remains MQTT 5 only.
- Preserve all QoS, ordering, Inflight, Receive Maximum, DUP and Session takeover semantics.
- Do not change benchmark matrix parameters to hide regressions.
- Do not optimize `MqttZeroRoutePlanner` without evidence that its microbenchmark regressed.
- Development gate remains every cell at or above Phase 0 throughput `95%` and at or below Phase 0 p99 `105%`.
- Strict MQTTnet gate remains recorded but does not block starting Phase 5 after the development gate passes.
- Steady-state hot paths must remain `0 B/op` after warmup.
- C# files use CRLF; one top-level type per file; all public APIs have XML comments; all if/method bodies use braces.
- All commands use PowerShell 7; all verification runs in the current worktree.

---

### Task 1: Benchmark Topology Controls and Reproducible A/B

**Files:**
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkTopologyOptions.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/Program.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BrokerBenchmarkJsonContext.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/BenchmarkTopologyOptionsTests.cs`

**Interfaces:**

- Produces: `BenchmarkTopologyOptions(int EventLoopShardCount, int IoQueueCount)`.
- Produces: `BenchmarkTopologyOptions Parse(Dictionary<string,string> arguments)`.
- CLI arguments are `--event-loop-shards <0..256>` and `--io-queue-count <1..256>`.
- `EventLoopShardCount=0` retains automatic logical-core resolution.
- `IoQueueCount=0` disables the explicit Kestrel override.

- [x] **Step 1: Write failing parser tests**

```csharp
[Fact]
public void ParseUsesAutomaticTopologyByDefault()
{
    BenchmarkTopologyOptions options =
        BenchmarkTopologyOptions.Parse([]);

    Assert.Equal(0, options.EventLoopShardCount);
    Assert.Equal(0, options.IoQueueCount);
}

[Fact]
public void ParseRejectsInvalidShardCount()
{
    Assert.Throws<ArgumentOutOfRangeException>(() =>
        BenchmarkTopologyOptions.Parse(
            new Dictionary<string, string>
            {
                ["event-loop-shards"] = "257"
            }));
}
```

- [x] **Step 2: Run RED**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~BenchmarkTopologyOptionsTests"
```

- [x] **Step 3: Implement parsing and server wiring**

`ServeAsync` sets `MqttZeroOptions.EventLoopShardCount`; when
`IoQueueCount > 0`, register
`services.Configure<SocketTransportOptions>(options => options.IOQueueCount = topology.IoQueueCount)`.
The same topology options are passed to `outbound` and `serve`.

- [x] **Step 4: Record topology in JSON**

Add `BenchmarkTopologyInfo` to the report environment with:

- configured Event Loop Shard count
- resolved Event Loop Shard count
- configured IOQueue count
- effective IOQueue count.

- [x] **Step 5: Verify and commit**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release
git commit -m "perf: add broker topology benchmark controls"
```

---

### Task 2: Fanout and Egress Stage Diagnostics

**Files:**
- Create: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs`
- Create: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnosticsSnapshot.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutOperation.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDispatcher.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.cs`
- Create: `tests/MqttZero.Tests/Broker/FanoutDiagnosticsTests.cs`

**Interfaces:**

- Produces: `MqttZeroFanoutDiagnosticsSnapshot GetSnapshot()`.
- `MqttZeroFanoutDiagnosticsSnapshot` contains:
  - PublishCount
  - LocalBatchCount
  - RemoteBatchCount
  - DeliveryCount
  - TotalFanoutTicks
  - MaxFanoutTicks
  - EgressSignalCount
  - EgressDrainCount
  - EncodedPublishCount
  - FlushWaitCount
  - MaxPendingQueueDepth.

- [x] **Step 1: Write failing aggregation tests**

```csharp
[Fact]
public void SnapshotCountsOnePublishAndItsBatches()
{
    MqttZeroFanoutDiagnostics diagnostics = new();
    diagnostics.RecordPublish(
        localBatchCount: 1,
        remoteBatchCount: 7,
        deliveryCount: 100,
        elapsedTimestampTicks: 50);

    MqttZeroFanoutDiagnosticsSnapshot snapshot =
        diagnostics.GetSnapshot();

    Assert.Equal(1, snapshot.PublishCount);
    Assert.Equal(7, snapshot.RemoteBatchCount);
    Assert.Equal(100, snapshot.DeliveryCount);
}
```

- [x] **Step 2: Run RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~FanoutDiagnosticsTests"
```

- [x] **Step 3: Implement counters without logging**

Use `Interlocked` counters. Instrument fanout completion, local/remote batch
completion, and Egress drain/flush transitions. Diagnostics are disabled
from JSON unless a benchmark explicitly requests them.

- [x] **Step 4: Verify and commit**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~FanoutDiagnosticsTests|FullyQualifiedName~Egress|FullyQualifiedName~FanoutDispatcherTests"
git commit -m "perf: add fanout and egress diagnostics"
```

---

### Task 3: QoS 0 Fanout Pipelining

**Files:**
- Modify: `src/MqttZero.Server/Routing/MqttZeroRoutePlannerLanes.cs`
- Create: `src/MqttZero.Server/Routing/MqttZeroRoutePlanBufferPool.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDispatcher.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.FastPath.cs`
- Create: `tests/MqttZero.Tests/Broker/QosZeroFanoutPipelineTests.cs`

**Interfaces:**

- `MqttZeroRoutePlanBufferPool.Rent()` returns an exclusive pooled planner buffer.
- `MqttZeroRoutePlanBufferPool.Return(buffer)` returns it after the operation completes.
- QoS 0 fanout may return after every target-Shard work item has been enqueued,
  not after every remote delivery completes.
- QoS 1/2 keep the existing operation-completion wait before PUBACK.

- [x] **Step 1: Write failing ordering test**

```csharp
[Fact]
public async Task QosZeroPublishesCanPipelineWithoutReorderingTargetShard()
{
    // Enqueue three QoS 0 publishes to the same target Shard while the
    // first batch is blocked. Release batches and assert [0, 1, 2].
}
```

- [x] **Step 2: Write failing buffer-owned test**

Assert that each in-flight QoS 0 operation owns a distinct planner buffer and
that buffers are returned exactly once after remote completion or fault.

- [x] **Step 3: Run RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~QosZeroFanoutPipelineTests"
```

- [x] **Step 4: Implement pipeline dispatch**

1. Rent an exclusive plan buffer for QoS 0.
2. Plan and retain the source Lease.
3. Enqueue at most one batch work item per target Shard.
4. Return to the caller after all target work items are enqueued.
5. Complete enqueue failure synchronously; report later delivery faults through the
   existing fanout fault callback.
6. Return the buffer and Lease only when all batch work completes.

- [x] **Step 5: Verify and commit**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~QosZeroFanoutPipelineTests|FullyQualifiedName~FanoutDispatcherTests|FullyQualifiedName~OutboundDeliveryTests"
git commit -m "perf: pipeline qos zero fanout"
```

---

### Task 4: Egress Coalescing and Large-Payload Flush

**Files:**
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`
- Create: `tests/MqttZero.Tests/Egress/EgressCoalescingTests.cs`

**Interfaces:**

- Add internal counters for data batches, encoded messages, bytes, Flush waits and
  maximum batch latency.
- Preserve control-frame priority.
- A non-completed Flush must yield the Shard; it must not spin or block.

- [x] **Step 1: Write failing coalescing test**

Queue three publishes for one connection before releasing the first Egress work;
assert one data drain processes all three messages and performs at most one Flush.

- [ ] **Step 2: Write failing large-payload test**

Publish 64 KiB to five connections; assert no extra full-Payload copy and one
Flush per Egress drain batch.

- [x] **Step 3: Run RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~EgressCoalescingTests"
```

- [x] **Step 4: Implement evidence-driven Egress changes**

If Task 2 shows very small data batches, coalesce signals within one Event Loop
drain before starting the Egress work. Do not add an artificial timer delay.
If Flush dominates, keep the existing nonblocking Flush continuation and reduce
duplicate Flush calls only when encoded bytes were actually written.

- [x] **Step 5: Verify and commit**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~Egress|FullyQualifiedName~OutboundDeliveryTests"
git commit -m "perf: coalesce egress hot path"
```

---

### Task 5: Topology Matrix and Regression Decision

**Files:**
- Modify: `eng/run-broker-benchmark.ps1`
- Create: `eng/compare-broker-topology.ps1`
- Modify: `perf/baseline/README.md`

**Interfaces:**

- `eng/compare-broker-topology.ps1` accepts:
  - `-EventLoopShards 1,2,4,8`
  - `-IoQueueCounts 1,2,4,8`
  - `-OutputDirectory artifacts/topology`
- It runs the fixed QoS 0/100/8 KiB and QoS 1/100/8 KiB cells only.

- [x] **Step 1: Add topology runner**
- [x] **Step 2: Run A/B**
- [x] **Step 3: Select the best topology or document that topology is not the cause**
- [x] **Step 4: Commit**

```powershell
git commit -m "perf: add broker topology comparison"
```

---

### Task 6: Development Matrix, Report and Phase Gate

**Files:**
- Create: `perf/baseline/broker-phase4.5-2026-11-25.json`
- Create: `perf/baseline/broker-phase4.5-2026-11-25.md`
- Modify: `perf/baseline/README.md`
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-11-25-phase-4-5-egress-hot-path.md`

- [x] **Step 1: Run the fixed 12-cell development matrix**

```powershell
dotnet run -c Release --no-build `
    --project benchmarks/MqttZero.BrokerBenchmarks -- `
    compare --client mqttnet --qos 0,1 --subscribers 1,100 `
    --payload 32,8192,65536 --warmup 2 --seconds 6 `
    --repetitions 3 --max-pending 1024 --start-port 39000 `
    --output artifacts/broker-phase4.5-dev.json
```

- [x] **Step 2: Compare against Phase 0**

```powershell
./eng/compare-broker-benchmarks.ps1 `
    -Baseline perf/baseline/broker-phase0-2026-11-25.json `
    -Current artifacts/broker-phase4.5-dev.json `
    -MaxThroughputRegression 0.05 `
    -MaxP99Regression 0.05
```

- [x] **Step 3: Run the official baseline**

```powershell
./eng/run-broker-benchmark.ps1 `
    -Client mqttnet `
    -OutputPath perf/baseline/broker-phase4.5-2026-11-25.json `
    -StartPort 30000
```

- [x] **Step 4: Write the report**

The report must list every red cell and state whether Phase 5 is unblocked.
Do not claim MQTTnet strict-gate success unless all 12 cells satisfy the strict
throughput and p99 conditions.

- [x] **Step 5: Full verification**

```powershell
dotnet restore MqttZero.slnx --locked-mode
dotnet build MqttZero.slnx -c Release --no-restore
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-build
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release --no-build
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-build
./eng/verify-coverage.ps1
./perf/verify-allocation.ps1
dotnet format MqttZero.slnx --verify-no-changes --no-restore
git diff --check
```

- [x] **Step 6: Commit**

```powershell
git add benchmarks eng src tests perf/baseline README.md docs/superpowers/plans/2026-11-25-phase-4-5-egress-hot-path.md
git commit -m "perf: close phase four hot path"
```

## Acceptance

- Benchmark topology and diagnostics are repeatable.
- The QoS 0/100/8 KiB bottleneck has a measured attribution.
- QoS 0 publishing remains per-Session ordered and Lease-safe.
- No new per-message hot-path allocations.
- All 12 development cells pass before Phase 5 starts.
- Any remaining strict MQTTnet failures are recorded explicitly.

## Execution Status

Blocked at the performance gate on 2026-11-25.

- Topology and diagnostics: complete.
- QoS 0 fanout pipelining: complete and ordered.
- Low-priority Egress and allocation-free enqueue: complete.
- Development matrix: `8/12 PASS`; four cells remain red.
- Strict MQTTnet gate: `0/12 PASS`.
- Large-Payload suppression work: not completed.
- Phase 5: not unblocked.
