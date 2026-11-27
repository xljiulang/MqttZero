# 架构与当前状态

本文件说明 MQTTZero 的运行时结构、内存所有权与当前已知状态，供二次开发和运维评估使用。
面向决策者的概述见仓库根 `README.md`；性能口径与对比见
`docs/performance-positioning.md` 与 `perf/baseline/`。

## 运行时结构

```text
Kestrel 监听（TCP / TLS / WebSocket）
  └─ MqttZeroClientConnection   每连接一个读循环和一个 PipeWriter
       └─ Session Shard         Client ID 稳定哈希到分片；会话状态只在该分片内变更
            ├─ Route Epoch      不可变路由快照（Topic Trie + 共享组游标）
            ├─ Route Planner    在发布者分片匹配订阅，按目标分片聚合 fanout
            ├─ Timer Wheel      每 Shard 四层分层时间轮，管理全部到期事件
            └─ Session Egress   每 Session 出站所有者：控制帧优先、数据帧按批预算编码
                                 └─ PipeWriter → Kestrel 传输
```

- 分片数量由 `MqttZeroOptions.EventLoopShardCount` 决定，`0` 表示逻辑处理器数（上限 256）；
  Kestrel 的 `IOQueueCount` 由宿主自行配置，与分片数无自动联动。
- 读循环只在所属分片上解码、路由和入队；跨分片投递通过分片调度器执行，不在读线程上同步等待。
- CONNECT 完成前和最终关闭阶段的少量写入由连接自身完成；附着 Session 后，控制报文与
  PUBLISH 数据都由该 Session 的 `MqttZeroSessionEgress` 串行写入，避免并发写者。

## 路由

- 订阅变更在分片上串行提交，成功后发布新的 `MqttZeroRouteEpoch`；读路径只读快照，不加全局锁。
- Topic Trie 采用路径级 Copy-on-Write，重订阅只复制受影响路径；共享订阅按 group + filter 维护轮询游标。
- 一次 PUBLISH 只做一次路由规划，按目标分片输出 `MqttZeroRoutePlan`，再由 fanout 分发器并行投递。

## 出站与背压

- `MqttZeroSessionEgress` 是 Session 出站的唯一所有者：控制帧走优先队列，数据帧按
  `OutboundBatchMessages` / `OutboundBatchBytes` / `OutboundBatchDuration` 组批编码。
- 同一 Session 最多存在一个未完成的 `PipeWriter.FlushAsync`；Flush 完成回调只清理
  Flush 状态并把执行权交回所属 Shard。
- 会话 `PendingMessages` 队列有上限（`MaxMessageQueueLength`），溢出时按 QoS 淘汰最旧消息并通知 Observer。
- 未确认的 QoS 1/2 消息计入 `InflightMessages`，受 `MaxInflight` 与客户端 Receive Maximum 双重约束。

## 内存所有权

- `MqttZeroBufferPool` 以 `ArrayPool<byte>` 为底层来源执行全局字节预算（`MaxBufferedBytes`）；
  预算耗尽时暂停读取并等待归还，不静默回退到普通数组。
- `MqttZeroMessageLease` 是引用计数的消息所有权单元：Topic、Properties、Payload 以
  `ReadOnlySequence<byte>` 暴露，在线投递、离线队列、Inflight 与 Retained 共享同一份内存。
- `BorrowFrom` 借用的租约指向调用方内存，跨异步步骤保留前必须 `EnsureOwned()`；
  外部 Store 保存二进制字段时必须复制，不能引用 Broker 的池化租约。

## 会话与持久化

- 会话状态（订阅、Pending、Inflight、Will、到期时间）只在所属分片上变更。
- Session 不再持有 `SyncRoot` 或 Mutation Lease；管理 API 读取原子发布的生命周期、
  订阅数组和计数快照，Route Epoch 只读取不可变快照。
- 三个 Store 分别负责会话、消息与 Retained，写穿语义：Save/Delete 成功后才 ACK，
  失败时回滚内存状态并关闭相关连接。
- 默认 Noop Store 支持同进程断线重连，不跨进程恢复，也不复制运行时 Payload。

## 维护与到期

- 每个 Shard 持有一个四层、每层 512 槽的侵入式分层时间轮，承载 KeepAlive、Will、
  Session Expiry、Message Expiry 与 Retained Expiry。
- 时间轮以 `KeepAliveCheckInterval` 为基础 Tick，不遍历全部 Session 或 Retained，也不创建
  逐消息 `Task`、`Timer` 或闭包。
- 到期节点带版本号；过期槽触发时若 Deadline 已变化，则按当前值重新挂入。

## 已知差距与已否决方案

| 事项 | 结论 | 报告 |
|---|---|---|
| 通用 Connection Writer / 强制公平调度 / 自适应 Direct-Backlog / 复用 Flush awaiter | 均未通过门禁，已移除；保留诊断与基准口径修正 | `perf/baseline/egress-closure-final-2026-11-26.md` |
| 全局 Packet Writer 池 | 顺序与 128 并发下均无收益，不引入 | `perf/baseline/packet-writer-pool-0.5.0-alpha.1.md` |
| 实验性 `SocketAsyncEventArgs` 传输 | p99 略好但吞吐更低，未同时改善，已删除；Kestrel 仍是唯一生产传输 | `perf/baseline/broker-transport-ab-2026-11-25.md` |
| 大载荷高扇出吞吐与尾延迟 | 相对 MQTTnet 仍有差距，未纳入发布门禁声明 | `docs/performance-positioning.md` |
| Session Egress + 每 Shard 时间轮 + Session 去锁 | 正确性、分配和供应链门禁通过；同日同机开发矩阵回退超过 5%，性能门禁阻断 | `perf/baseline/shard-owned-egress-timers-2026-11-26.md` |

## 当前阻断项

- 当前分支的架构闭环已完成，但同日同机开发矩阵中 MQTTZero 吞吐回退超过 `5%`。
- MQTTnet 对照在本次运行中也有明显漂移，尚不能把全部原始回退归因于单一代码变更。
- 下一步必须用交替 A/B、相同 Broker/client 进程和分阶段诊断区分机器热状态、Benchmark
  漂移与 Shard Egress/时间轮实现成本。
- 调查和性能门禁通过前，该分支不合并、不发布，也不作性能领先声明。

## 相关文档

- `docs/mqtt5-properties.md`：报文与属性端到端矩阵。
- `docs/performance-positioning.md`：性能定位、发布门禁与已知差距。
- `docs/performance-ab-harness.md`：同机 A/B 基准口径与命令。
- `perf/baseline/README.md`：所有历史基准记录索引。
- `CONTRIBUTING.md`：代码规范、测试与提交流程。
