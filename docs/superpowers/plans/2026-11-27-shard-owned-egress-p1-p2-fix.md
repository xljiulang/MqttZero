# Shard-Owned Egress 分支 P1+P2 修复记录

对应评审：`docs/superpowers/reviews/2026-11-27-shard-owned-egress-architecture-review.md`
分支：`refactor/shard-owned-egress-timers`（本轮未合并 `master`）

## 已完成

| 项 | 提交内容 | 验证 |
|---|---|---|
| P1-1 排空信号代际竞态 | 新增 `MqttZeroPendingDrain`：`Begin` 在每连接锁内返回本回合 token，`Complete(token)` 只接受当前 token，陈旧 token 既不减计数也不完成信号；`MqttZeroInboundWork`、`MqttZeroAcknowledgementWork` 携带 token 而不是整数代际 | `PendingDrainTests` 6 例（陈旧 token、每回合独立、1000 回合、8 线程并发） |
| P1-2 Timer Stop/Tick | `StopAsync` 只禁止新 Tick，把最终 `Clear()` 交给 `OnTickWorkReleased` 在所属分片执行；无 Tick 在途时才在调用线程清理；`TickAsync` 在级联循环检查 stopping；Tick 入队失败路径补 `Clear()` | `StopClearsScheduledNodesAndIsIdempotent`、`StopCompletesWhileTicksAreScheduled` |
| P1-3 单连接故障隔离 | `HandleInboundShardFailure` 改为 `HandleConnectionWorkFailure`，只记日志；work 内 `CloseAsync` 失败回退 `Abort()`；fanout 故障不再触发 Broker `StopAsync` | 新增 `InboundWorkFailureKeepsTheBrokerRunning`；`BackgroundFanoutFailureStopsNewConnections` 改为 `BackgroundFanoutFailureKeepsTheBrokerRunning` |
| P1-4 QoS 0 顺序 | 直连路径要求 `PendingPublishCount == 0`，判定提取为 `MqttZeroQosZeroDirectRouting.CanRoute` | `QosZeroDirectRoutingTests` 5 例；`DeferredQosZeroBlocksLaterDirectPublishes` 端到端顺序用例 |
| P2-1 控制饿死 | 每轮最多 64 个控制帧，达预算后强制一次数据批次；预算判定为 `MqttZeroEgressFairness` | `MqttZeroEgressFairnessTests` 12 例 |
| P2-2 PUBACK 移交 | 非分片控制投递支持 fire-and-forget：标记 actor 释放、用池化 `MqttZeroControlPostWork` 转移帧并立即返回；仅 `SendPubAckAsync` 使用，其余控制帧仍等待写入 | `OffShardControlPostReturnsBeforeTheFrameIsWritten`；`AttachedConnectionRoutesControlPacketsThroughEgress` 改为按帧计数 |
| P2-4 ACK 池快照 | 两个 ACK work 池暴露 `PoolCountForTest`，进入 `MqttZeroObjectPoolSnapshot`、基准探针与 `CountObjects` | `BrokerBenchmarks.Tests` 71/71，`BrokerMemorySnapshotTests` 断言两项计数在容量范围内 |
| P2-3 共享游标 | cursor 由 `MqttZeroSharedRouteEntries` 持有，planner 与 epoch match 直接调用 `group.Cursor.NextOffset`；注册表留在 broker，经 `MqttZeroRouteEpochManager` 注入并按引用计数修剪；`Empty` 每次返回新注册表，`ResetToEmpty` 复用 broker 注册表 | 790/790 测试；`CursorSurvivesEquivalentEpochRebuild` 改为 Remove/Add 重建；新增 `RemovedGroupNoLongerMatchesOrKeepsItsCursor`；分配哨兵 23/23 |

## 本轮未做

- **P2-5 Inflight 恢复并入批量路径**：需要给 `SendPublishBatchAsync` 增加 `duplicate` 参数并在首轮 drain 组装恢复批次，同样留待下一轮。

## 偏差与限制

- **P1-2 的阻塞式交错测试未落地**：让 Tick 回调阻塞后再调用 `StopAsync` 会挂死 Microsoft.Testing.Platform 测试宿主（诊断文件显示停在 `StopAsync()` 入口），因此改用"stop 清理已调度节点"与"stop 在 Tick 已调度时完成"两个用例；交错本身通过把 `Clear()` 移入 Tick 释放路径在结构上消除。
- **P1-4 端到端用例不能复现原竞态**：单分片下两条路径都要等同一个被阻塞的分片，顺序恰好正确；确定性保障来自 `MqttZeroQosZeroDirectRouting` 的单元测试。
- **行为变化**：交付级 fanout 异常不再把 Broker 置为 `Stopping`（评审 P1-3 的要求）。依赖旧行为的 `BackgroundFanoutFailureStopsNewConnections` 已被替换。
- **P1-1 成本**：`Begin`/`Complete` 从 Interlocked 对改为每连接锁；连接线程与分片线程竞争时可能多一次上下文切换，代价远小于一次网络发布。
- **P2-3 语义**：`MqttZeroRouteEpoch.Empty` 改为属性，每次访问给出独立注册表，避免测试之间共享游标；broker 侧改用 `ResetToEmpty()` 以保留自己的注册表。

## 验证

- `dotnet build MqttZero.slnx -c Release`：0 警告 0 错误。
- `MqttZero.Tests`：789/789。
- `MqttZero.BrokerBenchmarks.Tests`：71/71。
- `perf/verify-allocation.ps1`：23/23，热路径分配哨兵通过。
- 未跑同机 A/B；分支是否可合并仍取决于下一轮吞吐 ≥0.95×、p99 ≤1.05× 的门禁。
