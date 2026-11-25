# Phase 4.5 / Phase 7-Lite Transport Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用同一套 MQTTZero Broker 核心，对 Kestrel 传输与实验性 SocketAsyncEventArgs TCP 传输做可重复 A/B，确定当前端到端瓶颈是否来自 Kestrel 传输。

**Architecture:** 在 MqttZero.Server 内引入内部 Transport Connection 边界，Kestrel 仍是生产默认传输。Benchmark 程序集增加仅用于诊断的明文 TCP Socket 传输，使用 Pipe 所有权、SocketAsyncEventArgs 和池化缓冲区；Broker 的协议、路由、Shard、Egress 和会话语义保持完全相同。实验传输不承诺 TLS、WebSocket 或公开 API。

**Tech Stack:** .NET 10、Kestrel、System.IO.Pipelines、SocketAsyncEventArgs、MQTT 5、MQTTnet Client、xUnit v3、PowerShell 7。

**Spec:** `docs/sharded-actor-broker-plan.md` Phase 7；`perf/baseline/broker-phase4.5-2026-11-25.md`。

## Global Constraints

- 目标框架固定 `net10.0`，MQTTZero 只支持 MQTT 5。
- Kestrel 保持生产默认传输和默认 Benchmark 路径。
- 实验 Socket 传输只支持明文 TCP，不实现 TLS、WebSocket、客户端证书或公开 NuGet API。
- Broker 不得复制协议、路由、Shard、Egress 或 Session 逻辑；Kestrel 与 Socket 必须调用同一连接接受入口。
- 不对 Route Trie、Route Planner 或 Benchmark 矩阵做无证据修改。
- 正式性能参数保持不变：QoS `0/1`、订阅者 `1/100`、Payload `32/8192/65536`。
- A/B 判定使用同机同进程启动方式、相同 GC/ThreadPool 配置和相同 MQTTnet Client。
- Socket 原型没有稳定收益时，记录结果并从工作树删除实验传输代码。
- C# 文件使用 CRLF；一个文件一个顶层类型；public API 必须有 XML 注释；所有 if 和方法体使用大括号。
- 所有命令使用 PowerShell 7；所有验证在当前工作树执行。

---

### Task 1: 内部 Transport Connection 边界

**Files:**
- Create: `src/MqttZero.Server/Clients/MqttZeroTransportConnection.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs`
- Create: `tests/MqttZero.Tests/Broker/MqttZeroTransportConnectionTests.cs`

**Interfaces:**
- Produces: `internal sealed class MqttZeroTransportConnection`.
- Produces: `PipeReader Input`, `PipeWriter Output`, `EndPoint? RemoteEndPoint`, `bool IsSecure`, `bool IsWebSocket`, `bool AllowPacketFragmentation`.
- Consumes: existing `MqttZeroBroker.AcceptConnectionAsync` behavior without protocol changes.

- [ ] **Step 1: 写失败测试**

验证合法构造保留全部传输属性，null Pipe Reader/Writer 被拒绝。

- [ ] **Step 2: 运行 RED**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~MqttZeroTransportConnectionTests"
```

Expected: 编译失败，提示 `MqttZeroTransportConnection` 不存在。

- [ ] **Step 3: 实现边界并委托现有连接循环**

现有 public `AcceptConnectionAsync` 重载创建边界对象并委托内部核心方法；Kestrel Handler 和实验传输都进入同一核心。

- [ ] **Step 4: GREEN 并提交**

```powershell
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --filter "FullyQualifiedName~MqttZeroTransportConnectionTests|FullyQualifiedName~BrokerIntegrationTests.Transport"
git commit -m "refactor: add broker transport boundary"
```

### Task 2: Transport 选择与可观测配置

**Files:**
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkTransportKind.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkTransportSelection.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkTopologyInfo.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkTopologyOptions.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/Program.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/BenchmarkTransportSelectionTests.cs`

**Interfaces:**
- Produces: `BenchmarkTransportKind { Kestrel, Socket }`.
- Produces: `BenchmarkTransportKind BenchmarkTransportSelection.Parse(IReadOnlyDictionary<string,string>)`.
- CLI: `--transport kestrel|socket`，默认 `kestrel`。
- JSON: `BenchmarkTopologyInfo.Transport` 明确记录选择值。

- [ ] **Step 1: 写解析失败测试**

覆盖默认 Kestrel、大小写无关 Socket 和未知值拒绝。

- [ ] **Step 2: 运行 RED**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~BenchmarkTransportSelectionTests"
```

- [ ] **Step 3: 实现选择并贯串 serve/compare**

父进程把选择传给 Broker 子进程，只有 MQTTZero 子进程使用该选择；MQTTnet 始终使用自身 Kestrel 入口。

- [ ] **Step 4: GREEN 并提交**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~BenchmarkTransportSelectionTests|FullyQualifiedName~BenchmarkTopologyOptionsTests"
git commit -m "perf: add broker transport selection"
```

### Task 3: SocketAsyncEventArgs TCP 实验传输

**Files:**
- Create: `benchmarks/MqttZero.BrokerBenchmarks/SocketBrokerTransportServer.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/SocketBrokerConnection.cs`
- Create: `benchmarks/MqttZero.BrokerBenchmarks/SocketAsyncOperation.cs`
- Create: `tests/MqttZero.BrokerBenchmarks.Tests/SocketBrokerTransportTests.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/Program.cs`

**Interfaces:**
- Produces: `SocketBrokerTransportServer.StartAsync(MqttZeroBroker broker, IPEndPoint endpoint, bool allowPacketFragmentation, CancellationToken)`.
- Produces: `SocketBrokerTransportServer.StopAsync()`.
- `SocketBrokerConnection` owns one receive `SocketAsyncEventArgs`, one send `SocketAsyncEventArgs`, input/output Pipes and one active socket.
- Receive writes directly into `PipeWriter.GetMemory()` when it is array-backed, then `Advance` and `FlushAsync`.
- Send reads `ReadOnlySequence<byte>` from the output Pipe and writes array-backed memory through `SocketAsyncEventArgs`.

- [ ] **Step 1: 写端到端失败测试**

启动实验 Socket Broker，使用 MQTTnet Client 完成 QoS 1 订阅和发布，并验证订阅者收到消息。

- [ ] **Step 2: 运行 RED**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~SocketBrokerTransportTests"
```

- [ ] **Step 3: 实现最小传输**

使用 `Socket.NoDelay = true`、有界输入/输出 Pipe、最多一个未完成 receive 和 send、连接关闭取消、Socket fault 传播。Socket 模式使用 Generic Host 启动 Broker，不启动 Kestrel。

- [ ] **Step 4: GREEN 并提交**

```powershell
dotnet test tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj -c Release --filter "FullyQualifiedName~SocketBrokerTransportTests|FullyQualifiedName~RawMqttClientTests"
git commit -m "perf: add socket transport prototype"
```

### Task 4: 可重复 Transport A/B 工具

**Files:**
- Create: `eng/compare-broker-transports.ps1`
- Modify: `eng/run-broker-benchmark.ps1`
- Modify: `perf/baseline/README.md`

**Interfaces:**
- `compare-broker-transports.ps1` runs Kestrel and Socket for the same fixed 12-cell development matrix.
- Each transport writes a separate `BenchmarkReport` JSON with explicit transport identity.
- The script prints per-cell MQTTZero throughput and p99 delta; MQTTnet remains in both reports as the shared reference.

- [ ] **Step 1: 增加脚本**
- [ ] **Step 2: 运行短 smoke**

```powershell
./eng/compare-broker-transports.ps1 -WarmupSeconds 1 -MeasurementSeconds 2 -Repetitions 1
```

- [ ] **Step 3: 运行固定开发矩阵**

```powershell
./eng/compare-broker-transports.ps1
```

- [ ] **Step 4: 提交工具**

```powershell
git add eng perf/baseline/README.md
git commit -m "perf: compare kestrel and socket transports"
```

### Task 5: 归因结论与决策

**Files:**
- Create when retained: `perf/baseline/broker-transport-ab-2026-11-25.json`
- Create: `perf/baseline/broker-transport-ab-2026-11-25.md`
- Modify: `perf/baseline/README.md`
- Modify: `README.md`
- Update this plan's checkbox state.

**Interfaces:**
- Decision threshold: Socket must improve the primary QoS 0/100/8 KiB throughput and p99 consistently, without causing a new red cell elsewhere.
- A single noisy win is not enough; repeat before retaining the prototype.

- [ ] **Step 1: 重复 A/B 至少三次**
- [ ] **Step 2: 对比逐单元格吞吐与 p99**
- [ ] **Step 3: 记录结论**
- [ ] **Step 4: 若无稳定收益，删除实验 Socket 传输与入口**
- [ ] **Step 5: 全量验证**

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

- [ ] **Step 6: 提交报告或删除原型**

## Acceptance

- Kestrel 和 Socket 通过同一 Broker 连接入口运行。
- Socket 原型使用 SocketAsyncEventArgs 和有界 Pipe 背压。
- A/B 的报告明确标记传输、拓扑、矩阵和失败数。
- 决策基于重复结果，不以单次 benchmark 宣称性能提升。
- 未达到稳定收益时，实验代码被删除而不是留在生产路径。

## Execution Status

Completed on 2026-11-25.

- Internal transport boundary retained.
- Experimental SocketAsyncEventArgs transport completed and passed QoS 1
  integration tests with zero failures.
- Twelve-cell smoke completed for both transports.
- Primary QoS 0/100/8-KiB cell completed three interleaved repetitions.
- Socket median throughput was about 11.1% lower while p99 was about 7.8%
  lower, so the prototype failed the simultaneous-improvement decision gate.
- Experimental transport, CLI selection and comparison script were deleted.
- Evidence is recorded in
  `perf/baseline/broker-transport-ab-2026-11-25.md`.
