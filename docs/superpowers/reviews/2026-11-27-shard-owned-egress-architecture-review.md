# MQTTZero Shard-Owned Egress Architecture Review

## 审查范围

- 分支：`refactor/shard-owned-egress-timers`
- 审查基线提交：`b4bdd38 perf: validate shard command regression recovery`
- `master` 对照点：`418aefc`
- 审查日期：2026-11-27
- 范围：架构、并发、生命周期、消息顺序、资源所有权、尾延迟路径和测试缺口
- 性质：只读代码审查；本文档不包含生产代码修复

## 总体结论

**架构方向成立，但当前分支暂不适合合并。**

Shard 所有权、不可变 Route Epoch、连接级 Publish 解耦、Egress Actor 和复用
Shard 命令的总体方向是合理的。当前没有发现 P0 级必然崩溃问题，Release
构建、核心测试、Broker Benchmark 测试、分配哨兵和打包均通过。

不过，现有测试没有覆盖四个关键并发交错场景。它们都位于连接关闭、Shard
 Tick、异步 Work 完成和 QoS 0 发送顺序附近，属于合并前应先闭环的 P1
正确性风险。

## 当前状态摘要

| 项目 | 结论 |
|---|---|
| 架构方向 | 成立，继续沿 Shard 所有权方向演进 |
| 当前可合并性 | 暂不合并 |
| P0 必然崩溃问题 | 未发现 |
| P1 高风险问题 | 4 项 |
| P2 性能与可维护性问题 | 5 项 |
| 现有自动化验证 | 通过，但缺少关键确定性交错测试 |
| 下一步 | 先修排空信号竞态，再处理 Timer、故障隔离和 QoS 0 顺序 |

## P1：合并前应先修复

### P1-1：QoS 0/PUBACK 排空信号存在代际竞态

位置：

- `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs:1244`
- `AcceptedConnectionContext.BeginPendingPublish()`
- `AcceptedConnectionContext.CompletePendingPublish()`
- `AcceptedConnectionContext.WaitForPendingPublishesAsync()`
- `AcceptedConnectionContext.BeginAcknowledgement()`
- `AcceptedConnectionContext.CompleteAcknowledgement()`
- `AcceptedConnectionContext.WaitForAcknowledgementsAsync()`

当前 `CompletePendingPublish()` 和 `CompleteAcknowledgement()` 都是先递减
计数，再读取当前 drain `TaskCompletionSource` 并完成它。可发生如下交错：

```text
1. 旧 Work 将 count 从 1 递减到 0。
2. 新 PUBLISH 将 count 增加到 1，并写入新的 drain TCS。
3. 旧 Work 读取当前 drain 字段，错误地完成新 TCS。
4. cleanup 看到 outstanding count 仍为 1，但等待者已被提前完成。
5. WaitForPendingPublishesAsync()/WaitForAcknowledgementsAsync()
   可能进入忙循环，或与仍存活的 Work 发生生命周期判断错误。
```

这不是单纯的测试假阳性，而是排空信号没有绑定到具体“代”的所有权问题。

修复方向：

- 在递减前捕获本代 `TaskCompletionSource`，保证旧 Work 只能完成旧代信号。
- 或改为带 generation 的可复用 drain signal。
- 新代开始时必须与原代建立明确的所有权边界。
- 添加确定性测试，强制“旧 Work 完成”和“下一次 Begin”按上述顺序交错。
- 1000 次断线/重连测试应断言 Work 计数、drain 状态和 Lease 均能归零。

验收重点：

- cleanup 不会因已完成的旧 drain 等待仍存活的新 Work。
- 新 Work 不会被旧 Work 提前完成。
- `WaitForPendingPublishesAsync()` 和
  `WaitForAcknowledgementsAsync()` 在交错下最终完成，不忙循环。

### P1-2：Timer Wheel 的 Stop 与正在执行的 Tick 存在数据竞争

位置：

- `src/MqttZero.Server/Scheduling/MqttZeroShardTimerWheel.cs:206`
- `MqttZeroShardTimerWheel.StopAsync()`
- `MqttZeroShardTimerWheel.OnTickWorkReleased()`
- `MqttZeroShardTimerWheel.Clear()`

`StopAsync()` 在调用线程直接调用 `Clear()`，但没有等待可能正在所属 Shard
执行的 `TickAsync()`。如果 Tick 已经迁桶、遍历 bucket 链表或处理级联节点，
Stop 同时清空 buckets，可能出现：

- bucket 链表的跨线程读写竞争。
- Timer 节点遗漏或重复处理。
- Shutdown 阶段状态不一致。
- Stop 完成信号早于 Tick Work 实际释放。

`_tickScheduled` 已经表达 Tick Work 是否仍在执行，应成为 Stop 的同步点。

修复方向：

- Stop 先禁止新的 Tick 调度。
- 若 `_tickScheduled != 0`，等待 `OnTickWorkReleased()`。
- 最终 `Clear()` 必须在所属 Shard 上、且确认没有活跃 Tick 后执行。
- `OnTickWorkReleased()` 负责最后阶段的清理与 Stop 完成信号。
- 添加“Tick Callback 被阻塞时并发 Stop”的确定性测试。

验收重点：

- Tick 正在级联和 Stop 并发时没有 bucket 链表竞争。
- 所有节点只被处理、迁移或取消一次。
- Stop 可以重复调用且最终只完成一次。

### P1-3：单连接错误会触发整个 Broker 停止

位置：

- `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs:560`
- `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs:1006`
- `MqttZeroBroker.HandleInboundShardFailure()`

QoS 0 快速 Work 和异步 PUBACK Work 共用
`HandleInboundShardFailure()`。该回调会记录错误并把 Broker 状态改为
`Stopping`，但 Work 自身已经能够关闭对应连接。

结果是：一个客户端异常、一次可恢复持久化失败或一次连接级协议错误，可能
把整个 Broker 标记为停止，而不是只隔离当前连接。

修复方向：

- 区分连接级可恢复错误和 Shard/调度器级内部故障。
- 连接级错误只关闭当前连接、释放该连接的 Lease 和 Work，不改变全局
  Readiness。
- Shard 队列损坏、调度器无法继续工作等全局状态错误才升级为 Broker
  `Stopping`。
- Message Store 故障是否应全局降级，必须由 Store 策略明确决定，不能由
  通用连接异常路径隐式决定。
- 添加测试证明：普通客户端失败只关闭单连接；调度器致命错误才改变 Broker
  状态。

### P1-4：QoS 0 在 Direct 与 Deferred 路径之间可能乱序

位置：

- `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs:297`
- `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs:571`
- `MqttZeroBroker.CanRouteQosZeroDirectly()`

同一连接连续发送 QoS 0 PUBLISH 时，第一条消息可能因为 Retain、复杂扩展链或
其他条件进入 Session Shard 的异步 Work；第二条简单消息随后满足 Direct
条件，在连接线程立即路由。

可能顺序：

```text
1. Retain=true 的 QoS 0 PUBLISH 进入 deferred Work，但尚未完成路由。
2. 后续普通 QoS 0 PUBLISH 走 Direct 路径。
3. 第二条消息先到达 Fanout Scheduler。
4. 同一连接、同一 Topic 的 QoS 0 消息顺序被破坏。
```

修复方向：

- 增加每连接 Publish Sequence Barrier；存在更早的 deferred publish 时，
  后续 PUBLISH 不得越过它。
- 临时低成本方案：`PendingPublishCount > 0` 时禁用 Direct QoS 0。
- 长期方案：Direct QoS 0、Pooled QoS 0、QoS 1 和 QoS 2 统一登记每连接
  单调 Publish Sequence；所有后续路径都检查前序消息是否已投递。
- 添加“Retain 后普通 QoS 0”和“复杂扩展路径后普通 QoS 0”的顺序测试。

验收重点：

- 同一连接内 QoS 0 到同一目标的投递顺序与输入顺序一致。
- Direct 路径的启用不会让后续消息越过任何 deferred 前序消息。

## P2：应尽快处理

### P2-1：控制队列可能饿死数据

位置：`src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs:263`

`MqttZeroSessionEgress.ExecuteAsync()` 会循环到控制队列为空：

```text
while (_controlQueue.Count > 0)
```

如果控制包生产者持续补充队列，数据 PUBLISH 可能长期得不到执行。控制队列
虽然有 1024 上限，但消费者持续跟进时未必触发溢出。

建议：

- 每轮控制处理增加硬预算，例如最多 64 个控制帧或 250 µs。
- 达到预算后强制执行一个数据 Batch 或让出 Shard。
- 保持控制包优先，但禁止控制流长期独占 Egress Turn。

### P2-2：QoS 1 PUBACK 仍走完整 Shard 往返

位置：`src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs:156`

QoS 1 发布准备已经直接在连接线程执行，但 PUBACK 仍经过
`PostControlAsync()`。当连接线程不是 Egress 所属 Shard 时，会调用通用
`InvokeAsync`，等待控制 Frame 写入和 Flush 完成。

结果：

- 入站 PUBLISH 已经解耦；
- 读取线程仍等待 PUBACK 网络写完成；
- 每个 PUBACK 仍有控制 Frame、Shard 调用和等待链。

这很可能是 QoS 1 尾延迟仍未理想的重要原因。

建议：

- 增加连接级 fast-post，只安全移交 Control Frame 所有权，不等待写入完成。
- Egress 生命周期负责 Frame 成功、失败、取消和释放。
- 保持“消息先成功进入入站调度队列，再发送 PUBACK”的协议语义。

### P2-3：Shared Subscription Cursor 仍是全局热结构

位置：`src/MqttZero.Server/Routing/MqttZeroSharedGroupCursors.cs:19`

每次匹配共享组都会执行：

- 全局 `ConcurrentDictionary` 查询；
- 对全局 cursor 的 `Interlocked.Increment`。

单个共享组高吞吐时，这会成为全局竞争点。

建议：

- 将 `MqttZeroSharedGroupCursor` 直接预绑定到 Route Epoch 的共享组节点。
- Epoch 重建时迁移旧 cursor。
- 删除逐消息字典查询和 AddOrUpdate 风格路径。

### P2-4：新增 ACK Work 池没有进入内存快照

位置：`src/MqttZero.Server/Broker/MqttZeroObjectPoolSnapshot.cs`

`MqttZeroAcknowledgementWork` 和
`MqttZeroAcknowledgementShardWork` 都有静态对象池，但没有
`PoolCountForTest`，也没有进入 `MqttZeroObjectPoolSnapshot`。

因此内存稳定性测试无法判断 PUBACK Work 是否泄漏。

建议：

- 将两个池纳入 Broker memory snapshot。
- 纳入 1000 次断线/重连后的对象池归零断言。
- 异常、取消、Stop 和重复完成路径都应覆盖。

### P2-5：Inflight 恢复没有复用批量发送路径

位置：`src/MqttZero.Server/Egress/MqttZeroSessionEgress.cs:372`

Session 重连时，`ResumeInflightMessagesAsync()` 先将 Inflight 复制到 List，
然后逐条调用 `SendPubRelAsync()` 或 `SendPublishAsync()`。

Inflight 数量较大时会：

- 产生逐消息 Flush；
- 绕过正常发送路径的 Batch 和预算控制；
- 维护与正常发送不同的第二条编码/发送路径。

建议：

- 让恢复消息重新进入统一 SelectBatch、EncodePublishBatch 和 Egress Actor
  路径。
- 保留 QoS 1/2 Inflight、DUP 和恢复顺序语义。

## 新架构想法

### 1. 连接级 Publish Sequencer

每条连接维护单调 Publish Sequence。Direct QoS 0、Pooled QoS 0、QoS 1 和
QoS 2 都登记 Sequence。任何后续路径不得越过尚未投递的旧 Sequence。

价值：

- 统一解决 Direct、Deferred、ACK 解耦和重试造成的复杂时序。
- 为同一连接 QoS 0/1/2 顺序提供单一、可测试的规则。

### 2. 单目标 Fast Fanout

若 Route Plan 只有一个目标订阅者，直接投递一个 `SingleDeliveryWork`。

价值：

- 跳过完整 Fanout Operation。
- 跳过多 Shard Batch 和统一 Completion。
- 最可能改善 QoS 0/1 单订阅 64 KiB 的吞吐与 p99。

### 3. Control/Data Fair Egress

控制和数据都保留优先规则，但每轮给控制一个硬预算。

价值：

- 保持控制包优先。
- 防止持续 Ping/ACK 无限拖延数据 PUBLISH。
- 与之前被否决的“强制公平”方案不同：控制仍优先，只禁止长期独占。

### 4. Shard-Owned Timer Diagnostics

通过 Shard Work 获取 Timer snapshot，避免跨线程直接读取 bucket。

建议记录：

- Tick 最大耗时。
- 级联次数。
- 最老 deadline。
- Tick 调度与执行间隔。

## 已验证的正面结果

- Shard 所有权和不可变 Route Epoch 的总体方向合理。
- ShardSet 停止顺序已修正为先 Producer、后 Consumer。
- QoS 0 大小包、QoS 1 关键吞吐已有明显恢复。
- 分配哨兵、Release build、核心测试、Broker Benchmark 测试和打包均通过。
- 新增代码符合单类型单文件、CRLF、字段命名和源码规范。
- 普通 off-Shard PUBLISH/PUBACK 不再逐消息创建
  `MqttZeroShardInvocation<T>` 或 `TaskCompletionSource<T>`。
- QoS 0 内存探针已从每 1000 次发布约 `1.8–2.2 MiB` 降到约
  `0.41–0.44 MiB`，接近回退前水平。

## 当前性能证据

当前分支相对 `master` 的同机交替 A/B 结果仍不稳定，并存在明显尾延迟回退。
例如：

| QoS | 订阅者 | Payload | 吞吐 | p99 |
|---:|---:|---:|---:|---:|
| 0 | 1 | 32 B | 0.99x | 8.09x |
| 0 | 1 | 64 KiB | 0.69x | 0.94x |
| 1 | 1 | 64 KiB | 1.06x | 1.70x |
| 1 | 100 | 64 KiB | 0.97x | 1.61x |

因此即使忽略并发风险，当前分支也没有满足并入 `master` 的性能门禁。

完整数据和原始 JSON：

- `perf/baseline/shard-command-hotpath-2026-11-27.md`
- `perf/baseline/shard-owned-egress-ab-investigation-2026-11-26.md`
- `artifacts/broker-ab/master-current-2026-11-27/`
- `perf/baseline/shard-command-hotpath-raw-2026-11-27/`

## 合并判断

当前结论：

- 不合并 `master`。
- 不提升版本。
- 不把当前分支作为已经完成的主线架构。
- 继续在当前分支开发，不创建 worktree。
- 先修复正确性 P1，再根据性能 A/B 决定是否继续保留当前热路径实现。

## 下次继续的顺序

### 第一项：修复排空 TCS 代际竞态

先写确定性 RED 测试，覆盖：

1. 旧 Work 递减到 0。
2. 新 Work 建立新代。
3. 旧 Work 尝试完成信号。
4. cleanup 必须等待新代。

测试同时覆盖 Pending Publish 和 Acknowledgement 两套信号。

### 第二项：修复 Timer Wheel Stop/Tick 竞态

使用阻塞 Tick Callback 构造确定性交错，证明 Stop 不跨 Shard 清空活跃
bucket。

### 第三项：隔离单连接故障与 Broker 全局故障

明确哪些异常只关闭连接，哪些异常触发 Broker Readiness 降级。

### 第四项：加入 QoS 0 顺序屏障

先采用最小安全修复：存在 deferred publish 时，后续 QoS 0 不进入 Direct
路径。顺序测试稳定后，再评估统一 Publish Sequencer。

### 第五项：控制/数据预算

控制队列保持优先，但每轮设置控制帧数或时间硬预算，之后至少执行一个数据
Batch 或让出 Shard。

### 第六项：QoS 1 PUBACK 移交

在正确性 P1 完成后，再评估“Control Frame 所有权移交但不等待网络 Flush”
的连接级 fast-post。

### 后续性能实验

在前六项通过确定性测试和分配哨兵后，再评估：

- Shared Cursor 预绑定到 Route Epoch。
- ACK Work 池内存快照。
- Inflight 恢复并入统一 Batch 发送。
- 单目标 Fast Fanout。

所有性能实验必须继续保持：

- 不修改公开 API、MQTT wire format、Dashboard 契约和配置默认值。
- 使用 MQTTnet Client 和 Kestrel Broker。
- 修改热路径前先补确定性测试和分配断言。
- 未通过吞吐、p99、分配和正确性门禁的实验直接删除。

## 关联文档

- `docs/sharded-actor-broker-plan.md`
- `docs/superpowers/plans/2026-11-26-shard-owned-egress-timers-delock.md`
- `perf/baseline/shard-command-hotpath-2026-11-27.md`
- `perf/baseline/shard-owned-egress-ab-investigation-2026-11-26.md`
