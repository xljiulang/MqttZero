# Phase 4.6 Egress Shard Batching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-connection Egress work scheduling with one low-priority scheduler per Event Loop Shard, honor the existing outbound batch options, and restore the 12-cell Phase 0 development gate without adding an artificial coalescing delay.

**Architecture:** Each Shard owns one reusable scheduler work item and a deduplicated queue of ready Egress Actors. An Actor can appear in the queue at most once. The scheduler drains a bounded number of Actors, while each Actor keeps its control-first state machine, per-connection ordering and nonblocking Flush continuation. Data batches use `OutboundBatchBytes`, `OutboundBatchMessages` and `OutboundBatchDuration` instead of hard-coded limits.

**Tech Stack:** .NET 10, IThreadPoolWorkItem, ConcurrentQueue, MQTT 5, Kestrel, xUnit v3, PowerShell 7.

**Spec:** `docs/sharded-actor-broker-plan.md` Phase 2; `perf/baseline/broker-phase4.5-2026-11-25.md`; `perf/baseline/broker-transport-ab-2026-11-25.md`.

## Global Constraints

- Do not add a fixed coalescing delay.
- Do not modify Route Epoch, Route Planner, Kestrel topology or the transport.
- No production public API changes.
- Existing batch options become effective; defaults remain `512 KiB`, `128` messages and `500 us`.
- Scheduler passes process at most 64 Actors or 250 us.
- Control frames remain ahead of data at MQTT packet boundaries.
- FlushPending must not block other Actors on the same Shard.
- QoS, Inflight, Receive Maximum, DUP, Session Takeover, persistence rollback and message expiry remain unchanged.
- Steady-state scheduling must not add per-message managed allocations.
- The performance completion gate is the 12-cell Phase 0 development matrix at 95% throughput and 105% p99; strict MQTTnet superiority is recorded but not required for this phase.

---

### Task 1: Egress Diagnostics and Benchmark Configuration

**Files:**
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnosticsSnapshot.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkEgressOptions.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/Program.cs`
- Modify: test files under `tests/MqttZero.Tests/Broker` and `tests/MqttZero.BrokerBenchmarks.Tests`

**Interfaces:**
- Produces: `MqttZeroFanoutDiagnostics.Reset()`.
- Produces scheduler and data-batch counters for passes, Actor wakeups, batches, bytes, selection time and Flush starts.
- Produces benchmark CLI options `--outbound-batch-bytes`, `--outbound-batch-messages`, `--outbound-batch-duration-us`.

- [ ] Write failing diagnostics reset and benchmark parser tests.
- [ ] Run RED.
- [ ] Implement counters, reset and benchmark configuration.
- [ ] Reset diagnostics after outbound warmup drain.
- [ ] Run GREEN and commit.

### Task 2: Per-Shard Egress Scheduler

**Files:**
- Create: `src/MqttZero.Server/Egress/MqttZeroEgressScheduler.cs`
- Create: `src/MqttZero.Server/Egress/MqttZeroEgressSchedulerWork.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Delete: `src/MqttZero.Server/Egress/MqttZeroEgressWork.cs`
- Modify: `src/MqttZero.Server/Scheduling/MqttZeroShardSet.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Create: `tests/MqttZero.Tests/Egress/MqttZeroEgressSchedulerTests.cs`

**Interfaces:**
- Produces: one `MqttZeroEgressScheduler` per Shard.
- Produces: `MqttZeroEgressScheduler.Signal(MqttZeroEgressActor actor)`.
- Produces: Actor state transitions for ready, executing, rescheduling and stopping.
- Consumes: existing low-priority `MqttZeroEventLoop` queue.

- [ ] Write failing scheduler deduplication, ordering, non-reentrancy, FlushPending concurrency and stop tests.
- [ ] Run RED.
- [ ] Implement Scheduler and Actor integration.
- [ ] Run Actor, Scheduler, Flush, Session takeover and lifecycle tests.
- [ ] Run allocation sentinel and commit.

### Task 3: Configured Data Batch Selection

**Files:**
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressDataBatch.cs`
- Modify: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`

**Interfaces:**
- Batch selection consumes the three existing public option values.
- `MqttZeroEgressDataBatch` tracks encoded bytes from `EncodedLength`.

- [ ] Write failing message-count, encoded-byte and duration tests.
- [ ] Run RED.
- [ ] Replace hard-coded limits with option-driven selection.
- [ ] Preserve state transition rollback and current-packet completion semantics.
- [ ] Run outbound, QoS and allocation tests.
- [ ] Commit.

### Task 4: Diagnostics and Performance Decision

**Files:**
- Create after measured runs: `perf/baseline/broker-phase4.6-2026-11-25.json`
- Create: `perf/baseline/broker-phase4.6-2026-11-25.md`
- Modify: `perf/baseline/README.md`
- Modify: `README.md`

- [ ] Run QoS 0/100/8-KiB and QoS 1/100/8-KiB diagnostics three times.
- [ ] Verify Actor Drain count falls by at least 30% relative to encoded messages.
- [x] Evaluate four batch configurations.
- [x] Run the best configuration through the full 12-cell 2 s / 6 s / 3-repetition matrix.
- [ ] Run full tests, coverage, allocation, format and `git diff --check`.
- [ ] Record all red cells honestly and commit.

## Acceptance

- One Scheduler Work exists per Shard and one Actor can be queued at most once.
- FlushPending and control priority behavior remain correct under real PipeWriter tests.
- The three existing outbound batch options are effective.
- The diagnostic target reduces Actor Drain frequency by at least 30%.
- The Phase 4.6 report either records `12/12` development-gate success or explicitly keeps the phase red.

## Execution Status

Red on 2026-11-25.

- Scheduler, batch options and diagnostics are implemented.
- The `QoS 0 / 100 / 64 KiB` warmup stall was traced to a lost data signal
  while the connection had a pending data Flush.
- The 12-cell three-repetition matrix now completes without a drain timeout.
- Strict MQTTnet gate: `2/12` passed.
- Same-machine Phase 0 development gate: approximately `5/12` passed.
- Full report: `perf/baseline/broker-phase4.6-2026-11-25.md`.
