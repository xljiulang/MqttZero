# Phase 2 Connection Egress Actor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace Session Outbound Pump and Connection Writer with one Shard-owned Egress Actor.

**Architecture:** Each attached connection owns one reusable Egress Work. Control work is prioritized between data batches, data drains directly from Session Pending, and Flush completion signals the same Actor without creating a per-connection queue task.

**Tech Stack:** .NET 10, Phase 1 Event Loop Shards, PipeWriter, MQTT 5, xUnit v3.

**Spec:** `docs/sharded-actor-broker-plan.md` Phase 2.

## Constraints

- Keep QoS 0/1/2, Inflight, Receive Maximum, DUP, Expiry, persistence transitions and takeover semantics.
- Add `OutboundBatchBytes=512 KiB`, `OutboundBatchMessages=128`, `OutboundBatchDuration=500 us`.
- Duration limits selection/encoding, not network Flush.
- Control queue capacity is 1024; overflow aborts the connection and completes waiters with an exception.
- Delete `MqttZeroSessionOutboundPump`, `MqttZeroConnectionWriter`, fixed writer arrays and the second queue.
- Phase 2 must recover all five Phase 1 red cells to Phase 0 95% throughput / 105% p99 tolerance.

## Tasks

### Task 1: Options and Egress Data Structures

- Add three public batching options and validation.
- Add pooled encoded control frame and reusable data batch.
- Add tests for defaults, bounds, pooling and maximum packet size.

### Task 2: Egress Actor and Work

- Add `MqttZeroEgressActor`, `MqttZeroEgressWork` and state enum.
- Coalesce signals through CAS scheduling.
- Prioritize queued control work, then drain at most one data batch per activation.
- Signal again when Pending or Inflight work remains.

### Task 3: Nonblocking Flush

- Add flush state and completion continuation.
- FlushPending returns control to the shard; continuation only clears state and signals.
- Verify another connection can run while one is FlushPending.

### Task 4: Control Frames

- Route all connection control operations through the Egress control path.
- Bound control queue at 1024.
- Abort and fail all waiters on overflow or shutdown.
- Preserve maximum packet size and problem-information trimming.

### Task 5: Data Drain

- Move data selection from Broker pump into Egress data selector.
- Remove Session `OutboundBatch` and `ExpiredMessages`.
- Preserve Inflight transition, expiry, too-large, eviction and Receive Maximum.
- Resume inflight on Attach.

### Task 6: Lifecycle Cleanup

- Replace pump start/stop with Egress attach/stop.
- Migrate Delivery, PUBACK/REC/COMP, maintenance, management and takeover signal points.
- Delete old Pump, Writer, write lock and OutboundPump tests.

### Task 7/8: Performance and Verification

- Run Phase 2 development matrix and compare against Phase 0/1.
- Run all tests, coverage, allocation, format and diff checks.
- Record red or recovered cells without overstatement.

