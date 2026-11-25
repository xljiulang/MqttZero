# Phase 1 Event Loop Shard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or superpowers:subagent-driven-development. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 用可复用 `IThreadPoolWorkItem` Shard 调度替换 Inbound Dispatcher，并建立连接与 Session 的固定 Shard 所有权域。

**Architecture:** 每个 Shard 使用有界队列、CAS scheduled 标志和单一串行 Drain。CONNECT 前连接使用临时 Ingress Shard，CONNECT 后按 Client ID 固定到 Session Shard。旧 Inbound Dispatcher 直接删除。

**Tech Stack:** .NET 10、MQTT 5、`IThreadPoolWorkItem`、Kestrel、xUnit v3、PowerShell 7。

**Spec:** `docs/sharded-actor-broker-plan.md` 的 Phase 1。

## Global Constraints

- `EventLoopShardCount` 默认 `0`，自动使用 `Environment.ProcessorCount`，范围 `0..256`。
- Shard 是逻辑串行所有权域，不绑定线程或 CPU 亲和性。
- 每个 Shard 同一时刻最多一个 Drain。
- 单轮最多 1,024 个 WorkItem 或 500 微秒。
- Shard 队列有界，满时异步背压。
- Session Shard 由 Client ID 稳定哈希决定。
- 阶段 1 保留 Session 锁、Outbound Pump、Connection Writer、Route Snapshot 和 Message Queue。
- 直接替换 `MqttZeroInboundDispatcher`，不保留运行时双路径。
- Kestrel `IOQueueCount` 默认对齐 Shard 数，用户后配置可覆盖。
- C# 使用 CRLF；public API 必须 XML 注释；单文件单顶层类型。
- Phase 1 性能回归门禁：每个 MQTTZero 开发矩阵单元格吞吐不低于 Phase 0 的 95%，p99 不高于 Phase 0 的 105%。

---

### Task 1: Shard Count 与物理核解析

- Create `MqttZeroShardCountResolver` and tests.
- Add `MqttZeroOptions.EventLoopShardCount = 0` and validation.
- Auto mode uses `Environment.ProcessorCount`, capped to `1..256`.

### Task 2: Event Loop Shard 核心

- Create `MqttZeroEventLoop : IThreadPoolWorkItem`.
- Create abstract `MqttZeroShardWork`.
- Add bounded enqueue, single serialized drain, 1,024 item / 500 us budget, stop and diagnostics.
- Support same-shard inline invocation without self-deadlock.

### Task 3: Shard Set 与 Broker 生命周期

- Create `MqttZeroShardSet` and snapshots.
- Start all shards after persistence restore.
- Stop intake, drain shards, close connections and dispose shards deterministically.

### Task 4: Session Shard 映射

- Create stable FNV-1a `MqttZeroSessionShardMap`.
- Add `MqttZeroSessionState.ShardId`.
- Apply mapping to new sessions, persistence restore, takeover and replacement.

### Task 5: CONNECT 后连接所有权转移

- Add `MqttZeroClientConnection.ShardId`.
- Add shard invocation with reentrancy detection.
- Use a temporary shard before CONNECT and transfer after Client ID resolution.
- Process each complete packet through its connection shard before advancing the input buffer.

### Task 6: 替换 Inbound Dispatcher

- Add `MqttZeroInboundShardWork`.
- Delete `MqttZeroInboundDispatcher` and old tests.
- Route inbound work through `connection.ShardId`.
- Preserve QoS ACK semantics, lease ownership and stop/restart behavior.

### Task 7: ConnectedClient API 与 Kestrel IOQueue

- Run connection-scoped management calls on the Session Shard.
- Auto-align `SocketTransportOptions.IOQueueCount` in `AddMqttZero`.
- Later explicit `SocketTransportOptions` configuration overrides automatic value.

### Task 8: 全量回归、性能比较与文档

- Add `eng/compare-broker-benchmarks.ps1`.
- Run Phase 1 development matrix and compare with Phase 0.
- Run all tests, coverage, allocation, formatting and diff checks.
- Record shard diagnostics and performance regressions without overstating success.

## Acceptance

- Session/connection shard assignment is stable.
- One connection processes packets in order on one logical shard.
- Inbound dispatcher has been removed.
- Shard queue backpressure, exceptions, stop and restart are verified.
- Kestrel IOQueue alignment is tested.
- All correctness and coverage gates pass.
- Performance regression does not exceed the 5% development limits.

