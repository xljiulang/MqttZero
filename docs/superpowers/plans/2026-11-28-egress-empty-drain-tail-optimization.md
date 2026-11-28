# MQTTZero Egress 空唤醒与尾延迟优化实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 在不扩大批次、不增加常驻线程和不改变 MQTT 语义的前提下，消除无数据 Egress 唤醒，并在有证据时缩短同 Shard 扇出到 Egress 的调度路径，使 `QoS1 / 100 / 8 KiB` 的三次 p99 中位数不高于 MQTTnet。

**Architecture:** 保留当前 Shard-owned Session Egress、控制包优先、单连接串行写和现有大包批次策略。先通过内部诊断证明空 Drain 与 Shard 调度等待的占比，再分别消除 ACK 后无待发消息的 Egress 信号，以及有条件地增加同 Shard 的 Egress 本地续排。每个行为改动独立提交、独立基准；没有可重复收益的实验立即删除。

**Tech Stack:** .NET 10、ASP.NET Core Kestrel、System.IO.Pipelines、MQTT 5、eMQTT-Bench 0.6.4、MQTTnet Server 对照、PowerShell 7、Bash

**Spec:** `perf/baseline/linux-dev-mqttnet-release-gate.md`、`perf/baseline/linux-dev-qos1-64k-diagnostics-2026-11-28.md`、`docs/performance/positioning.md`

## Global Constraints

- 当前基线是 `master@efeef2e`；本计划在 `perf/egress-tail-optimization` 分支实施，不使用 worktree。
- 不修改公开 API、MQTT wire format、Dashboard 契约、公开配置或配置默认值。
- 正式跨 Broker 对比只使用 eMQTT-Bench `0.6.4` 和 MQTTnet Server。
- Linux 开发服务器是唯一正式性能环境；本机只用于编译、单测和一次性诊断。
- `QoS1 / 100 / 8 KiB` 的目标是三次正式延迟档 p99 中位数 `<= MQTTnet`。
- `QoS1 / 500 / 64 KiB` 的吞吐不得低于 MQTTnet 的 `1.00x`，p99 不得比本轮基线恶化超过 `5%`。
- `QoS1 / 100 / 8 KiB` 和 `QoS1 / 1 / 64 KiB` 的吞吐回退不得超过 `5%`。
- 不扩大 `128 KiB / 2 条` 大包批次上限，不恢复全局 `preferLocal`，不引入统一 Connection Writer。
- 不在本计划中修改 QoS 0 正式口径、共享订阅路由、Timer Wheel 或 Message Store。
- 同步 Encode、Route、Enqueue、Signal 和 Egress 本地续排路径保持 `0 B/op`；诊断开启路径不纳入零分配预算。
- 每个任务结束时执行定向测试、Release build、核心测试、分配哨兵和格式检查。
- 用户已要求不运行 Dashboard 和 Vue 测试；最终只验证相关 .NET 打包和仓库外消费者构建。

---

## 当前短板证据

现有正式结果不是“全面落后”，而是三个不同问题：

| 场景 | 当前结果 | 结论 |
|---|---:|---|
| `QoS1 / 500 / 64 KiB` | 吞吐 `1.082x`，p99 `0.991x`，RSS `0.949x` | 已通过严格目标，内存问题基本关闭 |
| `QoS1 / 100 / 8 KiB` | 五轮 p99 `9 ms` 对 `8 ms`，比值 `1.125x` | 唯一已接受的严格 p99 例外 |
| `QoS1 / 1 / 64 KiB` | 吞吐 `0.983x`，p99 `3 ms` 对 `3 ms` | 在保护线内，但仍有少量吞吐差距 |

`QoS1 / 100 / 8 KiB` 诊断样本：

```text
发布：500 条，预热 50 条
投递：50000 次
EgressDataBatchCount：55000
EgressBatchMessageCount：55000
EgressFlushStartCount：55000
EgressDrainCount：110702
EgressSchedulerPassCount：12758
FlushWaitCount：0
MaxObservedSessionPendingQueueDepth：1
MaxObservedSessionInflightQueueDepth：2
ManagedHeapBytes：约 23.9 MB
AllocatedBytes：约 336.8 MB
```

这些数据说明：

1. 每个 Session 每次发布只收到一条消息，因此批次参数无法改善该单元格。
2. 所有 Flush 都同步完成，`MaxEgressFlushPendingCount = 0`；当前 p99 差距不是异步 Flush 等待。
3. `EgressDrainCount / EgressDataBatchCount` 约为 `2.01`。代码路径对每次入队和每次 QoS1 PUBACK 都执行 `SignalEgress`；当 ACK 后待发队列为空时，后一次 Egress Actor 执行没有任何数据。
4. 当前 QoS1 扇出仍在同一 Shard 上先完成 Fanout Batch，再通过 Egress Scheduler 队列取得 Actor 执行权。空唤醒会放大该队列的深度和等待时间。
5. 64 KiB 主目标的主要剩余成本是每订阅者必要 Payload 复制和每 Session Actor 执行，不是 BufferPool 保留。

问题优先级：

| 优先级 | 假设 | 证据强度 | 处理 |
|---|---|---:|---|
| P1 | ACK 后空待发队列仍唤醒 Egress Actor | 高 | 默认必做 |
| P2 | Fanout 完成到 Egress 执行之间多一次 Shard 队列中转 | 中 | 指标确认后实施 |
| P3 | 写门等待或 PipeWriter 同步 Flush 形成尾部尖峰 | 低 | 只在 P1/P2 未闭合时调查 |
| P4 | Routing、Shared Cursor 或 Timer Wheel 是当前瓶颈 | 低 | 本计划不做 |

---

### Task 1: 增加 Egress 空 Drain 与就绪等待诊断

**Files:**
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnosticsSnapshot.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressScheduler.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BrokerMemoryProbeSnapshot.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/Program.cs`
- Test: `tests/MqttZero.Tests/Broker/FanoutDiagnosticsTests.cs`
- Test: `tests/MqttZero.Tests/Egress/MqttZeroEgressSchedulerTests.cs`
- Test: `tests/MqttZero.Tests/Egress/MqttZeroEgressFlushTests.cs`

**Interfaces:**
- Produces: `RecordEgressReadyToExecute(long elapsedTicks)`
- Produces: `RecordEgressEmptyDataDrain()`
- Produces: `RecordEgressSynchronousFlush()`
- Produces: `RecordEgressSignalWithoutPending()`
- Produces snapshot fields:
  - `EgressReadyToExecuteCount`
  - `TotalEgressReadyToExecuteTicks`
  - `MaxEgressReadyToExecuteTicks`
  - `EgressEmptyDataDrainCount`
  - `EgressSynchronousFlushCount`
  - `EgressSignalWithoutPendingCount`

- [x] **Step 1: 先写失败的诊断累计测试**

在 `FanoutDiagnosticsTests.cs` 增加：

```csharp
[Fact]
public void DiagnosticsTrackEmptyEgressAndSchedulerWait()
{
    MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

    diagnostics.RecordEgressSignalWithoutPending();
    diagnostics.RecordEgressReadyToExecute(40);
    diagnostics.RecordEgressReadyToExecute(7);
    diagnostics.RecordEgressEmptyDataDrain();
    diagnostics.RecordEgressSynchronousFlush();

    MqttZeroFanoutDiagnosticsSnapshot snapshot =
        diagnostics.GetSnapshot();
    Assert.Equal(1, snapshot.EgressSignalWithoutPendingCount);
    Assert.Equal(2, snapshot.EgressReadyToExecuteCount);
    Assert.Equal(47, snapshot.TotalEgressReadyToExecuteTicks);
    Assert.Equal(40, snapshot.MaxEgressReadyToExecuteTicks);
    Assert.Equal(1, snapshot.EgressEmptyDataDrainCount);
    Assert.Equal(1, snapshot.EgressSynchronousFlushCount);
}
```

- [x] **Step 2: 运行测试并确认失败**

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~FanoutDiagnosticsTests.DiagnosticsTrackEmptyEgressAndSchedulerWait"
```

预期：因新增记录方法或快照字段不存在而编译失败。

- [x] **Step 3: 增加诊断字段和记录方法**

诊断关闭时每个方法在第一次 `_enabled` 判断后立即返回。开启时：

```csharp
public void RecordEgressReadyToExecute(long elapsedTicks)
{
    if (!_enabled)
    {
        return;
    }

    Interlocked.Increment(ref _egressReadyToExecuteCount);
    Interlocked.Add(
        ref _totalEgressReadyToExecuteTicks,
        elapsedTicks);
    InterlockedMax(
        ref _maxEgressReadyToExecuteTicks,
        elapsedTicks);
}
```

其余计数字段使用 `Interlocked.Increment`，并在 `GetSnapshot`、`Reset` 和 benchmark memory probe 映射中完整加入。

- [x] **Step 4: 记录无待发数据的 Signal 候选**

在 `MqttZeroBroker.SignalEgress` 中只增加观测，不改变行为：

```csharp
if (session.PendingMessages.Count == 0)
{
    _fanoutDiagnostics.RecordEgressSignalWithoutPending();
}

_fanoutDiagnostics.RecordEgressSignal();
egress.Signal();
```

该步骤的 `EgressSignalWithoutPendingCount` 是后续 Task 2 的对照基线，行为仍是原来的唤醒。

- [x] **Step 5: 记录无权执行数据的 Actor 空转**

在 `MqttZeroSessionEgress.DrainOwnedAsync` 进入恢复和批次逻辑前，如果既没有待恢复 Inflight，也没有 Pending 消息，则记录一次空数据 Drain 并返回 `false`：

```csharp
if (Volatile.Read(ref _resumePending) == 0 &&
    session.PendingMessages.Count == 0)
{
    services.Diagnostics.RecordEgressEmptyDataDrain();
    return false;
}
```

保留随后原有的 `Interlocked.Exchange(ref _resumePending, 0)` 逻辑，不能为诊断改变恢复顺序。

- [x] **Step 6: 记录 Scheduler 就绪等待**

在 `MqttZeroEgressScheduler.Signal` 成功执行 `TryMarkReady()` 后保存时间戳：

```csharp
egress.MarkReady(Stopwatch.GetTimestamp());
```

`MqttZeroSessionEgress` 增加 internal `MarkReady(long timestampTicks)` 和 `ConsumeReadyTimestampTicks()`。只有当前线程赢得 `TryMarkReady()` 时写时间戳；Egress Scheduler 在 `MarkExecuting()` 前消耗时间戳并向诊断记录差值。

- [x] **Step 7: 记录同步 Flush**

在 `BeginFlush` 的 `flush.IsCompletedSuccessfully` 分支确认 `FlushResult.IsCompleted == false` 后记录一次同步 Flush。不得在写者关闭的异常路径记录成功。

- [x] **Step 8: 运行定向测试**

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~FanoutDiagnosticsTests|FullyQualifiedName~MqttZeroEgressSchedulerTests|FullyQualifiedName~MqttZeroEgressFlushTests"
```

- [x] **Step 9: 运行诊断基准**

```powershell
$env:MQTTZERO_SSH_CONFIG = 'D:/ssh/tencent/linux-dev.json'
$env:ENABLE_FANOUT_DIAGNOSTICS = '1'
$env:ENABLE_MQTTZERO_MEMORY_PROBE = '1'
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency `
  -Qos 1 `
  -Subscribers 100 `
  -Payload 8192 `
  -Repetitions 1 `
  -StartPort 20100 `
  -Campaign egress-empty-drain-baseline
```

预期：结果完整，诊断中约出现 `55000` 次 `EgressSignalWithoutPending` 和 `EgressEmptyDataDrain`。如果两者接近零，停止 Task 2，重新调查 ACK/调度路径。

- [x] **Step 10: 运行基础门禁并提交**

```powershell
dotnet build -c Release --no-restore
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release --no-build
pwsh ./perf/verify-allocation.ps1
dotnet format --verify-no-changes --no-restore
git diff --check
git add src benchmarks tests
git commit -m "perf: instrument empty egress drains"
```

---

### Task 2: 禁止无待发数据的 Egress 唤醒

**Files:**
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Test: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Test: `tests/MqttZero.Tests/Broker/BrokerHotPathAllocationTests.cs`

**Interfaces:**
- Consumes: `RecordEgressSignalWithoutPending()`
- Preserves: `SignalEgress(MqttZeroSessionState session)`
- Invariant: 有新入队消息时 `PendingMessages.Count > 0`，仍必须 Signal；ACK、PUBCOMP、失败清理或未知确认在待发队列为空时不得 Signal。

- [x] **Step 1: 写 ACK 后空队列不唤醒的失败测试**

在 `OutboundDeliveryTests.cs` 增加直接调用内部确认路径的测试：

```csharp
[Fact(Timeout = 5_000)]
public async Task QosOnePubAckWithNoPendingDataDoesNotSignalEgress()
{
    // Arrange a QoS1 delivery until Inflight is registered and Pending is empty.
    long signalBefore =
        broker.GetFanoutDiagnosticsForTest().EgressSignalCount;
    long suppressedBefore =
        broker.GetFanoutDiagnosticsForTest()
            .EgressSignalWithoutPendingCount;

    await broker.ProcessAcknowledgementCoreAsync(
        receiver,
        subscriberConnection,
        new MqttZeroAcknowledgementView(
            MqttPacketType.PubAck,
            packetIdentifier,
            MqttReasonCode.Success),
        TestContext.Current.CancellationToken);

    MqttZeroFanoutDiagnosticsSnapshot snapshot =
        broker.GetFanoutDiagnosticsForTest();
    Assert.Equal(signalBefore, snapshot.EgressSignalCount);
    Assert.Equal(
        suppressedBefore + 1,
        snapshot.EgressSignalWithoutPendingCount);
}
```

- [x] **Step 2: 写“有待发消息时 ACK 必须继续排空”的回归测试**

使用 `MaxInflight = 1` 投递两条 QoS1 消息，确认第一条约在途、第二条仍在 Pending；处理第一条 PUBACK 后，第二条必须被发送且 `EgressSignalCount` 增加。禁止通过跳过所有 ACK Signal 实现该任务。

- [x] **Step 3: 运行测试并确认一条失败、一条通过**

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~QosOnePubAckWithNoPendingDataDoesNotSignalEgress|FullyQualifiedName~QosOnePubAckDrainsPendingMessage"
```

预期：空队列测试失败；同 Inflight 窗口积压测试在旧实现下通过。

- [x] **Step 4: 在统一 Signal 入口跳过空队列**

修改 `MqttZeroBroker.SignalEgress`：

```csharp
if (session.PendingMessages.Count == 0)
{
    _fanoutDiagnostics.RecordEgressSignalWithoutPending();
    return;
}

_fanoutDiagnostics.RecordEgressSignal();
egress.Signal();
```

该判断在 Session 所属 Shard 上执行，和 Pending 队列修改具有同一所有权，不使用跨线程快照。

- [x] **Step 5: 运行 ACK、QoS1/2、停止和分配测试**

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~OutboundDeliveryTests|FullyQualifiedName~BrokerHotPathAllocationTests|FullyQualifiedName~BrokerIntegrationTests.Acknowledgements|FullyQualifiedName~BrokerIntegrationTests.Qos"
pwsh ./perf/verify-allocation.ps1
```

- [x] **Step 6: 运行一次诊断淘汰门禁**

```powershell
$env:MQTTZERO_SSH_CONFIG = 'D:/ssh/tencent/linux-dev.json'
$env:ENABLE_FANOUT_DIAGNOSTICS = '1'
$env:ENABLE_MQTTZERO_MEMORY_PROBE = '1'
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency `
  -Qos 1 `
  -Subscribers 100 `
  -Payload 8192 `
  -Repetitions 1 `
  -StartPort 20200 `
  -Campaign egress-empty-drain-removed
```

接受条件：

```text
投递完整且失败为 0
EgressSignalWithoutPendingCount 约等于 55000
EgressSignalCount 不再包含这些 ACK 空唤醒
EgressDrainCount / EgressDataBatchCount <= 1.05
EgressEmptyDataDrainCount 接近 0
```

未满足任一项时回退本任务生产改动，不进入正式基准。

- [x] **Step 7: 运行正式目标格三次**

```powershell
Remove-Item Env:ENABLE_FANOUT_DIAGNOSTICS -ErrorAction SilentlyContinue
Remove-Item Env:ENABLE_MQTTZERO_MEMORY_PROBE -ErrorAction SilentlyContinue
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency `
  -Qos 1 `
  -Subscribers 100 `
  -Payload 8192 `
  -Repetitions 3 `
  -StartPort 20220 `
  -Campaign egress-empty-drain-latency
```

接受条件：

```text
QoE 覆盖率 >= 99%
投递完整且失败为 0
p99 中位数 <= MQTTnet
p95 不回退超过 5%
Broker CPU / 1000 投递不高于本轮基线 3%
```

- [x] **Step 8: 运行主目标和保护格快速回归**

```powershell
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Quick -Mode throughput -Qos 1 -Subscribers 500 -Payload 65536 `
  -StartPort 20300 -Campaign egress-empty-drain-target

pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency -Qos 1 -Subscribers 1 -Payload 65536 -Repetitions 1 `
  -StartPort 20320 -Campaign egress-empty-drain-guard
```

接受条件：

```text
主目标吞吐 >= MQTTnet 1.00x
主目标 p99 相对基线回退 <= 5%
保护格吞吐回退 <= 5%
两个单元格投递完整且失败为 0
```

- [x] **Step 9: 基础门禁并提交**

```powershell
dotnet build -c Release --no-restore
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release --no-build
pwsh ./perf/verify-allocation.ps1
dotnet format --verify-no-changes --no-restore
git diff --check
git add src tests
git commit -m "perf: skip empty egress drains after acknowledgements"
```

如果 Task 2 的三轮 p99 中位数已经 `<= MQTTnet`，跳过 Task 3，直接进入 Task 4。

---

### Task 3: 同 Shard Fanout 完成后直接执行 Egress 续排

仅在 Task 2 完成后，`QoS1 / 100 / 8 KiB` 仍未达到 p99 `<= MQTTnet`，且 Task 1 诊断显示就绪等待为主要剩余项时实施。

**Files:**
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressScheduler.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutScheduler.cs`
- Modify: `src/MqttZero.Server/Scheduling/MqttZeroShardSet.cs`
- Test: `tests/MqttZero.Tests/Egress/MqttZeroEgressSchedulerTests.cs`
- Test: `tests/MqttZero.Tests/Broker/FanoutDispatcherTests.cs`
- Test: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`

**Interfaces:**
- Changes: `MqttZeroFanoutScheduler` constructor adds `MqttZeroEgressScheduler egressScheduler`.
- Produces: `internal ValueTask<bool> DrainReadyActorsCurrentShardAsync(long budgetTicks, CancellationToken cancellationToken)`
- Consumes: existing `_ready` queue、`MarkExecuting()`、`ExecuteAsync()` 和 `OnExecutionReleased()`。
- Preserves: Egress Scheduler 的初始终态调度路径和停止语义；没有丢失唤醒或双重执行。

- [x] **Step 1: 写直接续排的确定性失败测试**

在 `MqttZeroEgressSchedulerTests.cs`：

1. 阻塞 Shard。
2. 创建一个 Egress Actor 并 Signal。
3. 放行 Shard 后调用 `DrainReadyActorsCurrentShardAsync`。
4. 断言 Actor 在 Egress Scheduler Work 获得执行前已经 Drain。
5. 再让原 Scheduler Work 执行，断言 Actor 不会执行第二次。

预期：新方法不存在时编译失败。

- [x] **Step 2: 抽取 Egress Scheduler 的单轮 Actor 循环**

将 `MqttZeroEgressScheduler.ExecuteAsync` 的 Actor 循环抽取为：

```csharp
private async ValueTask<bool> DrainReadyActorsAsync(
    long deadlineTimestamp,
    int maximumActors,
    CancellationToken cancellationToken)
```

返回 `true` 表示仍有 `_ready` 项或 Actor 补排；返回 `false` 表示当前队列已空。

- [x] **Step 3: 增加同 Shard 直接续排入口**

```csharp
internal ValueTask<bool> DrainReadyActorsCurrentShardAsync(
    long budgetTicks,
    CancellationToken cancellationToken)
{
    if (!Shard.IsCurrent)
    {
        throw new InvalidOperationException(
            "The egress scheduler must run on its owning shard.");
    }

    long deadline = Stopwatch.GetTimestamp() + budgetTicks;
    return DrainReadyActorsAsync(
        deadline,
        MaximumActorExecutionsPerPass,
        cancellationToken);
}
```

如果返回 `true`，立即调用现有 `Schedule()`；不得留下未调度的 Actor。

- [x] **Step 4: 在每个 Fanout Scheduled Batch 后直接续排**

`MqttZeroFanoutScheduler` 增加 100 微秒本地预算：

```csharp
private static readonly long LocalEgressFollowUpBudgetTimestampTicks =
    Math.Max(1, Stopwatch.Frequency / 10_000);
```

在 `batch.Operation.ExecuteBatchAsync` 的成功或失败清理之后调用：

```csharp
_ = await _egressScheduler
    .DrainReadyActorsCurrentShardAsync(
        LocalEgressFollowUpBudgetTimestampTicks,
        cancellationToken)
    .ConfigureAwait(false);
```

控制包仍由 Egress Actor 内部优先处理。该调用不改变 Fanout Batch 的消息顺序，也不等待 Flush 完成。

- [x] **Step 5: 运行并发、停止和所有权测试**

覆盖：

- Fanout Batch 与新旧 Egress Work 同时就绪时 Actor 只执行一次。
- 直接续排达到 100 微秒预算后只生成一个后续调度。
- 控制包仍先于数据 Batch。
- Shard Stop、Fanout Stop、Egress Stop、Takeover 和取消后 Lease 归零。
- QoS0/1/2 顺序不变，QoS1/2 Inflight 不重复登记。

```powershell
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release `
  --filter "FullyQualifiedName~MqttZeroEgressSchedulerTests|FullyQualifiedName~FanoutDispatcherTests|FullyQualifiedName~OutboundDeliveryTests|FullyQualifiedName~SessionEgressOwnershipTests"
pwsh ./perf/verify-allocation.ps1
```

- [x] **Step 5.1: 运行三轮目标延迟**

```powershell
$env:MQTTZERO_SSH_CONFIG = 'D:/ssh/tencent/linux-dev.json'
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency `
  -Qos 1 `
  -Subscribers 100 `
  -Payload 8192 `
  -Repetitions 3 `
  -StartPort 20400 `
  -Campaign egress-local-follow-up-latency
```

接受条件：

```text
p99 中位数 <= MQTTnet
p95 中位数 <= MQTTnet * 1.05
投递完整且失败为 0
```

- [x] **Step 5.2: 运行主目标与保护格**

```powershell
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode throughput -Qos 1 -Subscribers 500 -Payload 65536 `
  -Repetitions 3 -StartPort 20420 `
  -Campaign egress-local-follow-up-target

pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency -Qos 1 -Subscribers 1 -Payload 65536 -Repetitions 1 `
  -StartPort 20440 `
  -Campaign egress-local-follow-up-guard
```

接受条件：

```text
主目标吞吐 >= MQTTnet 1.00x
主目标 p99 相对 Task 2 基线回退 <= 5%
保护格吞吐回退 <= 5%
```

- [x] **Step 5.3: 未达收益时删除实验**

任一条件出现时删除 Task 3 的生产实现和测试，只保留调查结论：

```text
目标 p99 未下降至少 5%
主目标吞吐低于 MQTTnet
任一保护格吞吐回退超过 5%
出现新的丢唤醒、重复执行或 Lease 泄漏
```

- [x] **Step 6: 基础门禁并提交**

```powershell
dotnet build -c Release --no-restore
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release --no-build
pwsh ./perf/verify-allocation.ps1
dotnet format --verify-no-changes --no-restore
git diff --check
git add src tests
git commit -m "perf: drain local fanout egress without scheduler handoff"
```

---

### Task 4: 更新性能证据并收敛例外

**Files:**
- Create: `perf/baseline/linux-dev-egress-empty-drain-2026-11-28.md`
- Modify: `perf/baseline/linux-dev-mqttnet-release-gate.md`
- Modify: `docs/performance/positioning.md`
- Modify: `docs/superpowers/plans/2026-11-28-egress-empty-drain-tail-optimization.md`

**Interfaces:**
- 输入：Task 2 和按需 Task 3 的原始 JSON、三次或追加后的中位数、CPU/RSS、诊断计数。
- 输出：是否仍需保留 `QoS1 / 100 / 8 KiB` p99 例外。

- [x] **Step 1: 运行核心和 Benchmark 测试**

```powershell
dotnet restore --locked-mode
dotnet build -c Release --no-restore
dotnet test --project tests/MqttZero.Tests/MqttZero.Tests.csproj `
  -c Release --no-build
dotnet test --project tests/MqttZero.BrokerBenchmarks.Tests/MqttZero.BrokerBenchmarks.Tests.csproj `
  -c Release --no-build
pwsh ./perf/verify-allocation.ps1
dotnet format --verify-no-changes --no-restore
```

- [x] **Step 2: 运行最终定向性能矩阵**

必须包含：

```text
QoS1 / 100 / 8 KiB，latency，三次
QoS1 / 500 / 64 KiB，throughput，三次
QoS1 / 1 / 64 KiB，throughput，三次
QoS1 / 100 / 8 KiB，throughput，三次
```

任一结果存在缺失投递、失败、QoE 覆盖不足或未排空时，该单元格先完整重跑；再次失败则本分支不合并。

- [x] **Step 3: 运行打包和外部消费者验证**

```powershell
pwsh ./eng/verify-packages.ps1
dotnet list MqttZero.slnx package --vulnerable --include-transitive
dotnet list MqttZero.slnx package --deprecated
```

不运行 Dashboard 和 Vue 测试。

- [x] **Step 4: 写性能报告**

报告必须记录：

```text
host.json 与 host-after.json
提交 SHA
完整命令
每轮吞吐、p50、p95、p99、失败数
Broker CPU/1000 投递
Broker 峰值 RSS/PSS
EgressSignalCount
EgressSignalWithoutPendingCount
EgressDrainCount
EgressDataBatchCount
EgressEmptyDataDrainCount
EgressReadyToExecute 的 total/max
EgressSynchronousFlushCount
最终中位数和结论
```

- [x] **Step 5: 只在严格目标通过时移除例外**

若三次 p99 中位数 `<= MQTTnet`：

- 删除 `linux-dev-mqttnet-release-gate.md` 中的“显式例外”描述。
- 将 `QoS1 / 100 / 8 KiB` 标记为通过。
- 在 `positioning.md` 中声明该单元格已严格闭合，但不得扩展为全矩阵领先声明。

若仍失败：

- 保留例外；
- 本分支不合并；
- 不提升版本；
- 报告剩余差值和最新诊断，不继续无证据地增加批次或调度阈值。

- [x] **Step 6: 更新计划进度并提交证据**

将本计划中完成项从 `- [x]` 改为 `- [x]`，记录失败实验的删除情况。

```powershell
git add perf docs
git commit -m "perf: validate empty egress drain optimization"
```

---

## 验收矩阵

| 场景 | 吞吐门禁 | 尾延迟门禁 | 完整性 |
|---|---|---|---|
| `QoS1 / 100 / 8 KiB` | 相对基线回退 `<=5%` | p99 三次中位数 `<= MQTTnet` | 50,000 次投递完整 |
| `QoS1 / 500 / 64 KiB` | `>= MQTTnet 1.00x` | 相对 Task 2 基线回退 `<=5%` | 50,000 次投递完整 |
| `QoS1 / 1 / 64 KiB` | 相对基线回退 `<=5%` | 不劣于基线超过 `5%` | 100 次投递完整 |
| `QoS1 / 100 / 8 KiB` throughput | `>= MQTTnet 0.95x` | 记录但不作为延迟档结论 | 完整 |

## 停止和回滚条件

- Task 1 诊断显示 `EgressSignalWithoutPendingCount` 或 `EgressEmptyDataDrainCount` 接近 0。
- Task 2 后 `EgressDrainCount / EgressDataBatchCount` 仍高于 `1.05`。
- Task 2 的三次 p99 没有改善，且 CPU 或主目标吞吐出现可重复回退。
- Task 3 未让目标 p99 下降至少 `5%`。
- 任一实验导致主目标吞吐低于 MQTTnet。
- 任一保护格吞吐或 p99 回退超过 `5%`。
- 出现丢唤醒、重复 Actor 执行、重复 Lease Release、消息乱序或排空超时。

命中停止条件时删除对应生产实现，只保留可复现的诊断和报告；不合并 `master`。
