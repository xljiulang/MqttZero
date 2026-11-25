# Phase 3 Memory Density Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove fixed per-connection and per-Session allocations and move message ownership to one reference-counted slab without changing MQTT semantics.

**Architecture:** First replace the preallocated QoS 0 candidate ring with intrusive queue links. Then make `MqttZeroMessageLease` own Topic, Properties and Payload in one pooled slab while preserving all existing `ReadOnlyMemory`/`ReadOnlySequence` properties. Topic Alias and Shard caches are only added after the core message path passes behavior and allocation tests.

**Tech Stack:** .NET 10, MQTT 5, ArrayPool/MemoryPool, intrusive queues, xUnit v3, PowerShell 7.

**Spec:** `docs/sharded-actor-broker-plan.md` Phase 3.

## Global Constraints

- Keep all MQTT QoS, Inflight, Receive Maximum, DUP, Session Takeover, Message Expiry and persistence behavior.
- Do not use 100,000 connections as a release gate.
- Queue and Lease hot paths remain `0 B/op` after warmup.
- Cache memory is included in `MaxBufferedBytes`.
- C# files use CRLF, one top-level type per file, braces for all bodies, and XML comments for public APIs.

---

### Task 1: Intrusive Lazy QoS 0 Candidate Queue

**Files:**
- Modify: `src/MqttZero.Server/Sessions/MqttZeroMessageQueue.cs`
- Modify: `src/MqttZero.Server/Sessions/MqttZeroQueuedMessage.cs`
- Modify: `tests/MqttZero.Tests/Sessions/MessageQueueTests.cs`

**Interfaces:**
- `MqttZeroMessageQueue(int capacity)` allocates no array proportional to `capacity`.
- QoS 0 candidates are linked through queued-message fields and removed in O(1) when the message leaves the main queue.

- [x] **Step 1: Write failing allocation and eviction tests**

```csharp
[Fact]
public void LargeCapacityDoesNotPreallocateCandidateStorage()
{
    long before = GC.GetAllocatedBytesForCurrentThread();
    _ = new MqttZeroMessageQueue(1_000_000);
    long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

    Assert.InRange(allocated, 0, 1024);
}
```

Add a mixed QoS test asserting oldest QoS 0 is evicted first and candidate removal does not retain dequeued messages.

- [x] **Step 2: Run RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~MessageQueueTests"
```

- [x] **Step 3: Implement intrusive candidate links**

Add `Qos0Next`, `Qos0Previous` and `IsQos0Queued` to `MqttZeroQueuedMessage`. Maintain head/tail/count in the queue. Unlink QoS 0 messages from the candidate chain during normal dequeue, clear, expiry and eviction.

- [x] **Step 4: GREEN and commit**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~MessageQueueTests|FullyQualifiedName~QueuedMessageTests"
git commit -m "perf: make qos zero candidate queue intrusive"
```

---

### Task 2: Single-Slab Message Lease

**Files:**
- Modify: `src/MqttZero/Memory/MqttZeroMessageLease.cs`
- Modify: `tests/MqttZero.Tests/Memory/MemoryLeaseAllocationTests.cs`
- Modify: `tests/MqttZero.Tests/Memory/MqttZeroBufferPoolTests.cs`

**Interfaces:**
- Topic, Properties and Payload remain exposed as contiguous subranges of one slab.
- `CopyFrom`, `BorrowFrom`, `EnsureOwned`, `CreateOwnedCopy` and `Release` signatures remain unchanged.
- Release returns exactly one slab to the owning buffer pool.

- [x] **Step 1: Write failing single-slab tests**

Assert Topic, Properties and Payload are contiguous inside the same backing array and that releasing the Lease returns rented bytes to zero.

- [x] **Step 2: Run RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~MemoryLeaseAllocationTests|FullyQualifiedName~MqttZeroBufferPoolTests"
```

- [x] **Step 3: Implement packed slab**

Layout: Topic | Properties | Payload. `CopyFrom` copies each sequence once into the slab. `BorrowFrom` keeps borrowed single/multi-segment sequences until `EnsureOwned` promotes them into one slab. Remove the three separately pooled buffers.

- [x] **Step 4: GREEN and commit**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~Memory|FullyQualifiedName~OutboundDeliveryTests|FullyQualifiedName~QueuedMessageTests"
./perf/verify-allocation.ps1
git commit -m "perf: store message lease in one slab"
```

---

### Task 3: Lazy Topic Alias Table

**Files:**
- Create: `src/MqttZero.Server/Clients/MqttZeroTopicAliasTable.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`
- Create: `tests/MqttZero.Tests/Broker/TopicAliasAllocationTests.cs`

**Interfaces:**
- Alias table allocates storage on first alias assignment.
- Lookups after the table exists remain zero-allocation.
- Disposal returns every pooled alias buffer.

- [x] **Step 1: Write failing lazy allocation test**
- [x] **Step 2: Write alias set/get/overwrite/dispose tests**
- [x] **Step 3: Run RED**
- [x] **Step 4: Implement table**
- [x] **Step 5: GREEN and commit**

```powershell
git commit -m "perf: lazily allocate topic aliases"
```

---

### Task 4: Per-Shard Buffer Cache and Read Backpressure

**Files:**
- Create: `src/MqttZero.Server/Memory/MqttZeroShardBufferCache.cs`
- Modify: `src/MqttZero/Memory/MqttZeroBufferPool.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Lifecycle.cs`
- Create: `tests/MqttZero.Tests/Memory/MqttZeroShardBufferCacheTests.cs`

**Interfaces:**
- Each Shard owns a small cache of common slab sizes.
- Cache bytes are permanently charged against the buffer-pool budget.
- Exhaustion pauses the owning connection read loop and resumes through Shard Work.

- [ ] **Step 1: Write cache charge/reuse tests**
- [ ] **Step 2: Write exhaustion wait/resume tests**
- [ ] **Step 3: Implement cache and Shard resume work**
- [ ] **Step 4: Run latency/allocation regression**
- [ ] **Step 5: Commit**

---

### Task 5: Memory Gate and Report

**Files:**
- Create: `perf/baseline/memory-phase3-2026-11-25.md`
- Modify: `perf/baseline/README.md`
- Modify: `README.md`

- [x] **Step 1: Measure Session creation allocation before/after**
- [x] **Step 2: Measure 100/1000 Subscriber broker working set**
- [x] **Step 3: Run all correctness/allocation/coverage gates**
- [x] **Step 4: Record Phase 3 status without making a 100,000-connection claim**
- [x] **Step 5: Commit**

## Acceptance

- No per-Session array proportional to `MaxMessageQueueLength`.
- One slab per owned message Lease.
- Empty topic aliases allocate no fixed 257-entry table.
- Existing QoS, persistence and retained tests pass.
- Phase 3 report records actual memory and any deferred cache/backpressure work.

## Execution Status

- Intrusive QoS 0 queue: complete.
- Single-Slab Lease: complete.
- Lazy Topic Alias: complete.
- Per-Shard Buffer Cache and read backpressure: deferred pending evidence.
- Memory and correctness gates: complete.
- Phase 4.5 network bottleneck remains; Phase 5 remains blocked.
