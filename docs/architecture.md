# 架构与当前状态

本文件说明 MQTTZero 的运行时结构、内存所有权与当前已知状态，供二次开发和运维评估使用。
面向决策者的概述见仓库根 `README.md`；性能口径与对比见
`docs/performance-positioning.md` 与 `perf/baseline/`。

## 运行时结构

```text
Kestrel 监听（TCP / TLS / WebSocket）
  └─ MqttZeroClientConnection   每连接一个读循环、一个写门、一个 PipeWriter
       └─ Session Shard         Client ID 稳定哈希到分片；会话状态只在该分片内变更
            ├─ Route Epoch      不可变路由快照（Topic Trie + 共享组游标）
            ├─ Route Planner    在发布者分片匹配订阅，按目标分片聚合 fanout
            └─ Egress Actor     每连接出站队列：控制帧优先、数据帧按批预算编码
                                 └─ PipeWriter → Kestrel 传输
```

- 分片数量由 `MqttZeroOptions.EventLoopShardCount` 决定，`0` 表示逻辑处理器数；
  `AddMqttZero` 默认把 Kestrel `IOQueueCount` 对齐到同一数值。
- 读循环只在所属分片上解码、路由和入队；跨分片投递通过分片调度器执行，不在读线程上同步等待。
- 每个连接只有一个写者：控制报文与批量数据都经过该连接的写门，或在分片上串行执行。

## 路由

- 订阅变更在分片上串行提交，成功后发布新的 `MqttZeroRouteEpoch`；读路径只读快照，不加全局锁。
- Topic Trie 采用路径级 Copy-on-Write，重订阅只复制受影响路径；共享订阅按 group + filter 维护轮询游标。
- 一次 PUBLISH 只做一次路由规划，按目标分片输出 `MqttZeroRoutePlan`，再由 fanout 分发器并行投递。

## 出站与背压

- `MqttZeroEgressActor` 是连接出站的所有者：控制帧走优先队列，数据帧按
  `OutboundBatchMessages` / `OutboundBatchBytes` / `OutboundBatchDuration` 组批编码。
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

- 会话状态（订阅、Pending、Inflight、Will、到期时间）只在所属分片上变更；
  锁层级固定为 `_lifecycleGate` → `_routeMutationGate` → 会话锁。
- 三个 Store 分别负责会话、消息与 Retained，写穿语义：Save/Delete 成功后才 ACK，
  失败时回滚内存状态并关闭相关连接。
- 默认 Noop Store 支持同进程断线重连，不跨进程恢复，也不复制运行时 Payload。

## 维护与到期

- 全局到期最小堆（`MqttZeroDeadlineQueue`）承载 KeepAlive、Will、Session Expiry 与 Retained Expiry，
  每 `KeepAliveCheckInterval` 弹出一次到期项，不再全表扫描。
- 收包路径只更新最后收包时间；到期触发时按当前值重新登记，避免热路径写共享结构。

## 已知差距与已否决方案

| 事项 | 结论 | 报告 |
|---|---|---|
| 通用 Connection Writer / 强制公平调度 / 自适应 Direct-Backlog / 复用 Flush awaiter | 均未通过门禁，已移除；保留诊断与基准口径修正 | `perf/baseline/egress-closure-final-2026-11-26.md` |
| 全局 Packet Writer 池 | 顺序与 128 并发下均无收益，不引入 | `perf/baseline/packet-writer-pool-0.5.0-alpha.1.md` |
| 实验性 `SocketAsyncEventArgs` 传输 | p99 略好但吞吐更低，未同时改善，已删除；Kestrel 仍是唯一生产传输 | `perf/baseline/broker-transport-ab-2026-11-25.md` |
| 大载荷高扇出吞吐与尾延迟 | 相对 MQTTnet 仍有差距，未纳入发布门禁声明 | `docs/performance-positioning.md` |

## 未完成项

- Egress 发送闭环统一：Session Pending、Inflight、编码、PipeWriter 与 Flush 尚未收敛到单一所有权域。
- 每分片时间轮：当前使用全局到期堆，维护循环仍跨分片读取会话。
- 分片独占去锁：会话状态仍使用每会话锁，尚未做到分片内无锁。

## 相关文档

- `docs/mqtt5-properties.md`：报文与属性端到端矩阵。
- `docs/performance-positioning.md`：性能定位、发布门禁与已知差距。
- `docs/performance-ab-harness.md`：同机 A/B 基准口径与命令。
- `perf/baseline/README.md`：所有历史基准记录索引。
- `CONTRIBUTING.md`：代码规范、测试与提交流程。
