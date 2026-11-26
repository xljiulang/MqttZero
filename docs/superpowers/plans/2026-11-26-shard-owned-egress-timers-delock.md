# MQTTZero Shard-Owned Egress, Timer Wheel And Session De-Lock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one Session Shard the sole owner of all mutable Session and outbound state, replace the global deadline scanner with per-Shard hierarchical timer wheels, and remove `session.SyncRoot` completely without changing public APIs or MQTT behavior.

**Architecture:** Preserve Kestrel transport, immutable Route Epochs, pooled message Leases, the Shard Event Loop and bounded Egress control/data batching. Replace the current Egress actor plus its split Broker/Connection state with one Session Egress owner, then attach one hierarchical timer wheel to each Shard, then migrate all remaining Session mutation and management reads away from locks and onto Shard-local execution or immutable snapshots.

**Tech Stack:** .NET 10, C#, System.IO.Pipelines, Microsoft.Extensions.Logging source generation, xUnit v3, existing MQTTZero Event Loop, Route Epoch, Buffer Pool, Egress Scheduler and Broker Benchmark infrastructure.

**Spec:** `docs/sharded-actor-broker-plan.md`

## Global Constraints

- Work directly in a normal repository branch named `refactor/shard-owned-egress-timers`; do not create a worktree.
- Base the branch on current `master`.
- Public APIs, MQTT wire behavior, Dashboard contracts and configuration defaults remain unchanged.
- Keep `KeepAliveCheckInterval` as the timer-wheel base tick, default `500 ms`.
- Do not reintroduce the rejected universal Connection Writer, forced fairness, Direct/Backlog modes or Socket transport.
- Performance acceptance is correctness-first: no same-machine end-to-end throughput or p99 cell may regress by more than `5%`.
- Synchronous Egress Enqueue, Signal, Route and timer cancel/update paths must remain `0 B/op` after warmup.
- No new unbounded object pools, queues or static caches.
- All new production types are `internal`.
- Public and internal production types follow XML documentation, one top-level type per file, mandatory braces, CRLF, pooled large arrays and generated logging/JSON rules from `csharp-code-rules`.
- Do not raise the package version, merge `master` or push remote as part of this plan.

---

## Current Evidence And Decisions

- The current Egress actor already confines Flush completion to the owning Shard and prevents a second concurrent Flush.
- Pending/Inflight selection and PipeWriter writes remain split across `MqttZeroBroker.Delivery` and `MqttZeroClientConnection`.
- Broker control packets can still reach `MqttZeroClientConnection` through write-gate and fallback paths after connection establishment.
- `MqttZeroDeadlineQueue` is global, protected by its own lock and drained by one global `PeriodicTimer` maintenance loop.
- Every KeepAlive, Will, Session Expiry and Retained Expiry operation crosses the Broker maintenance task and `FindSession`.
- Session mutable state is protected by approximately eighty `lock (session.SyncRoot)` sites and a per-client Session Mutation Lock.
- The measured average Session lock wait is low, but maximum waits have exceeded one millisecond; the architectural goal is deterministic ownership rather than a speculative micro-optimization.
- Previous Egress adaptive and writer experiments failed their performance gates and were removed. This plan changes ownership and lifecycle only.

### Ownership Model

```text
MqttZeroEventLoop[shardId]
  -> owns MqttZeroSessionState mutations and MqttZeroSessionEgress
  -> owns MqttZeroShardTimerWheel
  -> runs SessionEgress data/control/Flush state machine
  -> publishes immutable management snapshots

Broker API / fanout / connection reader
  -> posts Shard work
  -> never mutates Session fields directly
```

---

## Task 1: Add Ownership Invariants And Failing Tests

**Files:**

- Create: `src/MqttZero.Server/Scheduling/MqttZeroShardOwnership.cs`
- Create: `tests/MqttZero.Tests/Scheduling/MqttZeroShardOwnershipTests.cs`
- Modify: `src/MqttZero.Server/Scheduling/MqttZeroEventLoop.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs`

**Interfaces:**

- Consumes: `MqttZeroEventLoop.IsCurrent`, `MqttZeroSessionState.ShardId`, current Egress tests.
- Produces: `MqttZeroShardOwnership.EnsureCurrent(int shardId, string operation)`.
- Produces diagnostics: `ShardOwnershipViolationCount`, `TimerTickCount`, `TimerCascadeCount`, `OldestTimerAgeMilliseconds`.

### Step 1: Write the failing ownership tests

- [ ] Add tests proving that an Event Loop child async operation, including `await Task.Yield()`, remains identified as the owning Shard.
- [ ] Add tests proving a different Shard or a non-Shard caller is rejected by `MqttZeroShardOwnership.EnsureCurrent`.
- [ ] Add a diagnostic test showing a rejected ownership operation increments the violation count without corrupting Session state.

### Step 2: Verify RED

Run:

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~ShardOwnership"
```

Expected: FAIL because the ownership type and diagnostics do not exist.

### Step 3: Add the ownership guard

- [ ] Add an internal static helper around the Event Loop's scoped Shard identity.
- [ ] Keep the helper allocation-free and synchronous.
- [ ] Use it only at internal mutation boundaries and deterministic tests; do not add one atomic operation per message.
- [ ] Add a diagnostic violation counter used only when fanout diagnostics are enabled.

### Step 4: Verify GREEN

Run the Task 1 command again and then:

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~MqttZeroEventLoopTests|FullyQualifiedName~FanoutDiagnosticsTests"
```

Expected: PASS with no allocation regression.

### Step 5: Commit

```powershell
git add src/MqttZero.Server/Scheduling src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs tests/MqttZero.Tests/Scheduling
git commit -m "test: define shard ownership invariants"
```

---

## Task 2: Make MqttZeroSessionEgress The Sole Outbound Owner

**Files:**

- Create: `src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs`
- Create: `src/MqttZero.Server/Egress/MqttZeroSessionEgressState.cs`
- Create: `src/MqttZero.Server/Egress/MqttZeroSessionEgressServices.cs`
- Create: `tests/MqttZero.Tests/Egress/SessionEgressOwnershipTests.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressScheduler.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressSchedulerWork.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.FastPath.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Packets.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Sessions.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`
- Modify: `src/MqttZero.Server/Sessions/MqttZeroSessionState.cs`
- Delete: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Delete: `src/MqttZero.Server/Egress/MqttZeroEgressActorState.cs`
- Modify: `tests/MqttZero.Tests/Egress/*.cs`
- Modify: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Modify: `tests/MqttZero.Tests/Broker/BrokerHotPathAllocationTests.cs`

**Interfaces:**

- Consumes: `MqttZeroEgressScheduler`, `MqttZeroClientConnection`, `MqttZeroSessionState`, Egress control frame pool, Batch Budget, Flush Operation.
- Produces:

```csharp
internal sealed class MqttZeroSessionEgress : IAsyncDisposable
{
    internal MqttZeroSessionEgressState State { get; }

    internal bool TryEnqueue(MqttZeroQueuedMessage message);

    internal ValueTask PostControlAsync(
        MqttZeroEgressControlFrame frame,
        CancellationToken cancellationToken);

    internal ValueTask ExecuteAsync(CancellationToken cancellationToken);

    internal ValueTask StopAsync();

    public ValueTask DisposeAsync();
}
```

- `MqttZeroSessionEgressServices` supplies persistence transitions, observer notifications, time, options, diagnostics and fault reporting without exposing mutable Broker collections.

### Step 1: Write failing Egress ownership tests

- [ ] Assert one Session has exactly one `MqttZeroSessionEgress`.
- [ ] Assert Pending selection, Inflight insertion, batch list clearing and QoS 0 release happen only during owner execution.
- [ ] Assert control frames are always written before the next data packet and only at complete MQTT packet boundaries.
- [ ] Assert there is never more than one incomplete `PipeWriter.FlushAsync`.
- [ ] Assert a synchronous Flush failure releases each QoS 0 QueuedMessage exactly once.
- [ ] Assert QoS 1/2 Inflight is registered before encoding and remains resendable after write/Flush failure.
- [ ] Assert Detach, Takeover and Broker Stop wait for owner shutdown before releasing Session resources.
- [ ] Assert all Enqueue/Signal/control fast paths remain `0 B/op` after warmup.

### Step 2: Verify RED

Run:

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~SessionEgressOwnership|FullyQualifiedName~OutboundDeliveryTests|FullyQualifiedName~BrokerHotPathAllocationTests"
```

Expected: compile/assertion failures because Broker Delivery and Connection still mutate outbound state.

### Step 3: Move outbound state into the owner

- [ ] Move control queue, control batch, data signal, FlushPending, Flush failure and stop state from `MqttZeroEgressActor` into `MqttZeroSessionEgress`.
- [ ] Move Pending selection, expiry removal, Overflow handling, Inflight insertion, TransitionQueuedToOutbound, encoded batch creation, Flush handoff and QoS 0 release from `MqttZeroBroker.Delivery` into the owner.
- [ ] Keep Egress Scheduler responsible only for ready-actor deduplication, Shard budget and invocation.
- [ ] Change `MqttZeroSessionState.Egress` and `MqttZeroClientConnection.AttachEgress` to use `MqttZeroSessionEgress`.
- [ ] Keep current batch thresholds: existing byte/message/duration options and large/small adaptive rules unchanged.
- [ ] Preserve a synchronous owner fast turn when already on the owning Shard and no Flush is pending.
- [ ] Route all post-CONNECT control/ACK writes through the owner or its Shard command path.
- [ ] Keep direct writes only during pre-attachment CONNECT/CONNACK and final connection closure.
- [ ] Remove old callback-based Actor tests after equivalent Session Egress coverage passes.

### Step 4: Verify GREEN

Run:

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~Egress|FullyQualifiedName~OutboundDelivery|FullyQualifiedName~BrokerLifecycle|FullyQualifiedName~Qos"
```

Expected: PASS, BufferPool/Lease counters return to zero, and no targeted allocation test changes.

### Step 5: Run the Egress regression cells

```powershell
dotnet benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet --qos 0,1 --subscribers 1,100 --payload 32,8192,65536 `
  --warmup 2 --seconds 6 --repetitions 2 --max-pending 1024 `
  --start-port 35000 --comparison-gate false
```

Expected: zero failures and complete delivery; every cell is within `5%` of the same-machine pre-refactor baseline.

### Step 6: Commit

```powershell
git add src/MqttZero.Server/Egress src/MqttZero.Server/Broker src/MqttZero.Server/Clients/MqttZeroClientConnection.cs src/MqttZero.Server/Sessions/MqttZeroSessionState.cs tests/MqttZero.Tests/Egress tests/MqttZero.Tests/Broker
git commit -m "refactor: consolidate outbound delivery under session egress"
```

---

## Task 3: Add One Hierarchical Timer Wheel Per Shard

**Files:**

- Create: `src/MqttZero.Server/Scheduling/MqttZeroShardTimerWheel.cs`
- Create: `src/MqttZero.Server/Scheduling/MqttZeroTimerNode.cs`
- Create: `src/MqttZero.Server/Scheduling/MqttZeroTimerTickWork.cs`
- Create: `src/MqttZero.Server/Scheduling/MqttZeroShardTimerSnapshot.cs`
- Modify: `src/MqttZero.Server/Scheduling/MqttZeroShardSet.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Deadlines.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Maintenance.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Lifecycle.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.cs`
- Modify: `src/MqttZero.Server/Sessions/MqttZeroSessionState.cs`
- Modify: `src/MqttZero.Server/Retained/MqttZeroRetainedMessage.cs`
- Delete: `src/MqttZero.Server/Scheduling/MqttZeroDeadlineQueue.cs`
- Delete: `tests/MqttZero.Tests/Scheduling/MqttZeroDeadlineQueueTests.cs`
- Create: `tests/MqttZero.Tests/Scheduling/MqttZeroShardTimerWheelTests.cs`
- Create: `tests/MqttZero.Tests/Broker/BrokerTimerWheelTests.cs`

**Interfaces:**

- Produces:

```csharp
internal sealed class MqttZeroShardTimerWheel : IAsyncDisposable
{
    internal int Count { get; }

    internal void ScheduleOrRemove(
        MqttZeroTimerNode node,
        long dueUtcTicks);

    internal void Cancel(MqttZeroTimerNode node);

    internal ValueTask TickAsync(
        long nowUtcTicks,
        CancellationToken cancellationToken);
}
```

- `MqttZeroTimerNode` stores owner kind/id, deadline version, due ticks, bucket position and intrusive links.
- Each Session lazily allocates at most one reusable node for KeepAlive, Will, Session Expiry and Message Expiry.
- Each retained message lazily allocates one reusable Retained Expiry node.

### Step 1: Write failing wheel tests

- [ ] Cover deadlines before, equal to and after the current base tick.
- [ ] Cover all four level boundaries and cascading into lower levels.
- [ ] Cover reschedule, cancel, stale version, reused node and clear during shutdown.
- [ ] Cover deterministic delivery of distinct due ticks in ascending order.
- [ ] Cover `uint`-maximum expiry duration without overflow.
- [ ] Cover FakeTimeProvider-driven KeepAlive, Will, Session Expiry, Message Expiry and Retained Expiry.
- [ ] Assert ticks execute only on the owning Shard.
- [ ] Assert no full Session or Retained collection is enumerated during a tick.

### Step 2: Verify RED

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~ShardTimerWheel|FullyQualifiedName~BrokerTimerWheel"
```

Expected: FAIL because timer-wheel types do not exist.

### Step 3: Implement the wheel

- [ ] Use four levels with `512` slots per level.
- [ ] Set level-0 duration to `KeepAliveCheckInterval`; spans are approximately `256 s`, `36 h`, `2.6 years`, and `1300 years`.
- [ ] Use intrusive linked nodes, no per-message `Task`, timer or closure.
- [ ] Use a reusable Tick Work posted by the Shard time source.
- [ ] Stop the Shard ticker before stopping Event Loop work.
- [ ] Replace all global `_deadlines.Schedule/Remove/Clear` calls with owning-Shard wheel operations.
- [ ] Replace `RegisterSessionDeadlines` locking with Shard-current mutation.
- [ ] Keep Message Expiry indexed only by the current Pending head; queue changes update that one node.
- [ ] Route Retained Expiry for a Topic to the stable hash of the Topic across Shards.
- [ ] Preserve `PendingDeadlineCount` as the sum of all wheel node counts.
- [ ] Delete `RunMaintenanceAsync`, `PeriodicTimer`, global `_deadlines` and `MqttZeroDeadlineQueue`.

### Step 4: Verify GREEN

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~TimerWheel|FullyQualifiedName~Deadlines|FullyQualifiedName~Retained|FullyQualifiedName~BrokerLifecycle"
```

Expected: PASS; all deadlines fire once, stale nodes are ignored and Stop leaves zero timer nodes.

### Step 5: Run timer scale and allocation tests

- [ ] Add a 10,000 detached Session test proving one due tick does not iterate the Broker Session dictionary.
- [ ] Assert Schedule/Update/Cancel are `0 B/op` after each node is created.
- [ ] Assert repeated Tick with no due nodes performs constant work and no allocation.

### Step 6: Commit

```powershell
git add src/MqttZero.Server/Scheduling src/MqttZero.Server/Broker src/MqttZero.Server/Sessions src/MqttZero.Server/Retained tests/MqttZero.Tests/Scheduling tests/MqttZero.Tests/Broker
git commit -m "perf: add per-shard hierarchical timer wheel"
```

---

## Task 4: Publish Lock-Free Management And Route State

**Files:**

- Create: `src/MqttZero.Server/Sessions/MqttZeroSessionLifecycleSnapshot.cs`
- Modify: `src/MqttZero.Server/Sessions/MqttZeroSessionState.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.ClientManagement.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Management.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.RouteEpochs.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Persistence.cs`
- Create: `tests/MqttZero.Tests/Sessions/SessionSnapshotConcurrencyTests.cs`
- Modify: `tests/MqttZero.Tests/Broker/BrokerIntegrationTests.ManagementLimits.cs`

**Interfaces:**

- Produces immutable `MqttZeroSessionLifecycleSnapshot` for connected state, clean start, expiry, disconnected time, Will and Will due time.
- Produces volatile Session references for `Connection`, `MqttZeroSessionEgress` and immutable subscription array.
- Produces atomic pending, Inflight and Inbound QoS 2 counts for management snapshots.

### Step 1: Write failing snapshot and management tests

- [ ] Concurrently mutate subscriptions and read `GetSessions`; every returned field set must come from one published lifecycle version.
- [ ] Concurrently connect, disconnect and call `GetClients`; no disposed connection is dereferenced.
- [ ] Run 1000 client ID takeovers while management polling; no deadlock, corrupted count or leaked Lease.
- [ ] Call management APIs from inside an owning Shard and prove no self-wait.

### Step 2: Verify RED

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~SessionSnapshotConcurrency|FullyQualifiedName~ManagementLimits"
```

Expected: current locking or inconsistent snapshot behavior fails at least one new test.

### Step 3: Implement immutable publication

- [ ] Replace mutable subscription list with an atomically swapped immutable array.
- [ ] Publish a new lifecycle snapshot on every connected-state, expiry, clean-start and Will change.
- [ ] Use volatile references for Connection and Session Egress.
- [ ] Update queue/Inbound counters atomically at the owning mutation site.
- [ ] Make `CreateSnapshot`, `CreateClientSnapshot` and `TryGetClient` lock-free.
- [ ] Build persistence snapshots while on the owning Shard, then await Store calls after the snapshot is copied.
- [ ] Keep management reads weakly consistent but internally coherent.

### Step 4: Verify GREEN and allocation

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~Snapshot|FullyQualifiedName~Management|FullyQualifiedName~Persistence|FullyQualifiedName~RouteEpoch"
```

Expected: PASS; management reads do not wait for Shards or Session locks.

### Step 5: Commit

```powershell
git add src/MqttZero.Server/Sessions/MqttZeroSessionState.cs src/MqttZero.Server/Sessions/MqttZeroSessionLifecycleSnapshot.cs src/MqttZero.Server/Broker tests/MqttZero.Tests/Sessions tests/MqttZero.Tests/Broker
git commit -m "refactor: publish lock-free session snapshots"
```

---

## Task 5: Move Every Session Mutation To Its Owning Shard

**Files:**

- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Deadlines.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Packets.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Sessions.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.ClientManagement.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs`
- Modify: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Modify: `tests/MqttZero.Tests/Broker/ConnectedClientUnitTests.cs`
- Modify: `tests/MqttZero.Tests/Broker/BrokerIntegrationTests.Sessions.cs`

**Interfaces:**

- Consumes: `MqttZeroShardOwnership`, Event Loop inline invocation and Task 2 Session Egress.
- Produces: all Session-mutating methods callable only from the owning Shard.

### Step 1: Write failing shard-confinement tests

- [ ] Assert Subscribe/Unsubscribe rejects or internally posts off-Shard calls.
- [ ] Assert PUBACK, PUBREC, PUBREL, PUBCOMP and ACK-triggered Inflight changes execute on the Session Shard.
- [ ] Assert Detach, Takeover, Will publishing, Session Expiry and Persistence rollback execute on the Session Shard.
- [ ] Assert Broker Publish and ConnectedClient methods do not self-wait when already on the target Shard.
- [ ] Assert close/abort from non-Shard threads only posts commands or changes atomics owned by `MqttZeroClientConnection`.

### Step 2: Verify RED

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~OutboundDelivery|FullyQualifiedName~ConnectedClient|FullyQualifiedName~Sessions|FullyQualifiedName~ShardOwnership"
```

Expected: at least one call path reports off-Shard Session mutation.

### Step 3: Route mutations through Shard work

- [ ] Replace direct Session access in the connection read loop with cached Shard delegates that execute inline or post Work.
- [ ] Route publisher ACK generation through the publisher Session Shard or Session Egress command path.
- [ ] Route management disconnect and connected-client operations through the target Shard.
- [ ] Route Timer handlers through the timer wheel's owning Shard.
- [ ] Remove connection write fallbacks after Session Egress attachment.
- [ ] Keep pre-CONNECT and connection-finalization writes in a separate pre-attached state.

### Step 4: Verify GREEN

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~BrokerIntegrationTests|FullyQualifiedName~OutboundDelivery|FullyQualifiedName~ConnectedClient|FullyQualifiedName~Management|FullyQualifiedName~Deadline"
```

Expected: PASS with zero ownership violations.

### Step 5: Commit

```powershell
git add src/MqttZero.Server/Broker src/MqttZero.Server/Clients/MqttZeroClientConnection.cs src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs tests/MqttZero.Tests/Broker
git commit -m "refactor: move session mutations onto owning shards"
```

---

## Task 6: Remove Session Locks And Mutation Leases

**Files:**

- Modify: `src/MqttZero.Server/Sessions/MqttZeroSessionState.cs`
- Modify: all production files containing `session.SyncRoot`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs`
- Modify: `tests/MqttZero.Tests/SourceConventionTests.cs`
- Create: `tests/MqttZero.Tests/Architecture/SessionLockRemovalTests.cs`
- Modify: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Modify: `tests/MqttZero.Tests/Broker/BrokerIntegrationTests.ManagementLimits.cs`

**Interfaces:**

- Consumes: successful Task 5 Shard routing.
- Removes: `MqttZeroSessionState.SyncRoot`.
- Removes: `SessionMutationLock`, `SessionMutationLease`, `AcquireSessionMutationAsync` and `_sessionMutationLocks`.

### Step 1: Write failing source and concurrency tests

- [ ] Add a Roslyn source test asserting `session.SyncRoot` and `_sessionMutationLocks` do not occur in `src/`.
- [ ] Add deterministic Route Enqueue and PUBACK concurrency tests.
- [ ] Add concurrent Detach and Takeover tests.
- [ ] Add concurrent management snapshot and 1000-reconnect tests.
- [ ] Assert Buffer Pool, Lease, QueuedMessage and Timer nodes return to zero after each conflict sequence.

### Step 2: Verify RED

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
  --filter "FullyQualifiedName~SessionLockRemoval|FullyQualifiedName~OutboundDelivery|FullyQualifiedName~ManagementLimits"
```

Expected: source test reports remaining `session.SyncRoot` sites.

### Step 3: Delete the locks

- [ ] Delete the Session `SyncRoot` property.
- [ ] Delete all `lock (session.SyncRoot)` blocks.
- [ ] Delete per-client Session Mutation Lock code and lock hierarchy comments.
- [ ] Keep only valid `_sessionGate` dictionary atomicity, lifecycle and route/retained mutation gates where they protect global structures.
- [ ] Change any caller that depended on Session locks into one of: Shard invocation, immutable snapshot publication, atomic count update or Session Egress command.
- [ ] Verify no lock escapes through generated code, tests or Benchmark reflection.

### Step 4: Verify GREEN

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release --no-restore
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --no-restore
```

Expected: all tests pass and the Session-lock source guard finds zero references.

### Step 5: Commit

```powershell
git add src tests/MqttZero.Tests
git commit -m "refactor: remove session synchronization locks"
```

---

## Task 7: Full Verification And Documentation

**Files:**

- Modify: `docs/sharded-actor-broker-plan.md`
- Modify: `docs/architecture.md`
- Modify: `docs/performance-positioning.md`
- Modify: `perf/baseline/broker-comparison-methodology.md`
- Create: `perf/baseline/shard-owned-egress-timers-2026-11-26.md`

### Step 1: Run allocation and correctness gates

```powershell
dotnet restore MqttZero.slnx --locked-mode
dotnet build MqttZero.slnx -c Release --no-restore
./perf/verify-allocation.ps1
dotnet format MqttZero.slnx --verify-no-changes --no-restore
```

Expected: PASS with zero warnings and zero allocations in the guarded hot paths.

### Step 2: Run development performance matrix

Run each QoS separately:

```powershell
dotnet benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet --qos <0|1|2> --subscribers 1,100 `
  --payload 32,8192 --warmup 2 --seconds 6 --repetitions 2 `
  --max-pending 1024 --start-port <unique-range> --comparison-gate false
```

For every cell:

- [ ] Failed operations and missing deliveries are zero.
- [ ] End-to-end throughput and publish-call p99 are within `5%` of the same-machine pre-change baseline.
- [ ] Run a third repetition only when two-run throughput spread exceeds `10%` or a run is invalid.

### Step 3: Run targeted 64 KiB safety cells

- [ ] QoS 0 / 1 / 64 KiB
- [ ] QoS 0 / 100 / 64 KiB
- [ ] QoS 1 / 1 / 64 KiB
- [ ] QoS 1 / 100 / 64 KiB

Expected: no cell regresses more than `5%`; no performance-leadership claim is made unless separately measured with three repetitions.

### Step 4: Supply-chain and package gates

```powershell
dotnet list MqttZero.slnx package --vulnerable --include-transitive
dotnet list MqttZero.slnx package --deprecated
./eng/verify-packages.ps1
```

Expected: no vulnerability/deprecation findings and all repository-external consumers build.

### Step 5: Document results

- [ ] Record machine, OS, .NET version, branch, base commit and exact commands.
- [ ] Record raw JSON paths and two-run medians.
- [ ] Record Egress owner, timer-wheel and Session lock diagnostics.
- [ ] Mark any rejected optimization and delete its production implementation.
- [ ] Update the sharded-actor plan checkboxes only after tests and gates pass.

### Step 6: Commit

```powershell
git add docs perf
git commit -m "docs: record shard-owned egress and timer architecture"
```

---

## Final Acceptance

- [ ] One active `MqttZeroSessionEgress` per Session.
- [ ] All outbound control/data writes and the only Flush lifecycle execute through that owner.
- [ ] QoS 0 release exactly once; QoS 1/2 Inflight and recovery semantics remain correct.
- [ ] One timer wheel per Shard; no global Deadline scan.
- [ ] No `session.SyncRoot` or `_sessionMutationLocks` in production code.
- [ ] Management APIs remain lock-free and cannot self-deadlock.
- [ ] 1000 reconnect/takeover tests report zero Lease, Queue, Frame, Timer or Buffer leaks.
- [ ] Allocation sentinels and complete test/build/format/package gates pass.
- [ ] No public API or wire-format change.
- [ ] No package version bump, master merge or remote push in this plan.
