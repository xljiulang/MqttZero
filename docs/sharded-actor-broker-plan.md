# MQTTZero Sharded Actor Broker 性能重构计划

## 状态

- 状态：Phase 2、5、6、8 的架构闭环已在实验分支完成；正确性、分配、构建和
  供应链门禁通过；同日同机性能回归门禁未通过，因此分支被阻断，不能合并或发布。
- 当前分支：`refactor/shard-owned-egress-timers`
- 目标框架：`net10.0`
- 记录日期：2026-11-26
- 当前证据：
  `perf/baseline/shard-owned-egress-timers-2026-11-26.md`、
  `perf/baseline/shard-owned-egress-ab-investigation-2026-11-26.md`
- 下一步：替换 `d14f433` 引入的逐 PUBLISH 跨 Shard
  `InvokeAsync` 分配路径，并用真实 off-Shard PUBLISH 分配测试和完整性能矩阵验收；
  修复前不合并 `master`，不提升版本。

> 本文保留总体架构设计和历史任务追踪。当前可执行顺序、前置门禁和停止条件
> 以上述跨 Shard PUBLISH 根因为准；下方未勾选历史项不应在没有新证据和新计划授权时直接恢复执行。

## 背景与目标

当前实现的编解码、消息 Lease、常规 Topic Trie 和稳态零分配路径已经具备较好基础。
以下是本计划立项时识别的主要瓶颈，其中 Phase 1、2、3、4、5、6 的已实施部分关闭
了 Egress 所有权、全局 Deadline 扫描和 Session 锁问题：

- Egress 状态曾分散在 Broker Delivery、Egress Actor 和 ClientConnection；
  现已收敛到每 Session 一个 `MqttZeroSessionEgress`。
- 单发布者通常固定在一个 Inbound Dispatcher Worker 中，无法利用全部核心完成扇出。
- 订阅变更会遍历并重建全部 Session 路由。
- 共享订阅和 Retained 订阅匹配仍包含线性扫描。
- 每个 Session 和连接存在与流量无关的固定大数组分配。
- KeepAlive、Will、Session 和 Retained 清理曾经每 500 ms 全量扫描；现已改为
  每 Shard 一个分层时间轮。

架构目标已调整为“固定 Shard 所有权 + 每 Session Session Egress + 每 Shard 时间轮 +
不可变路由快照”。实现先以确定性所有权和去锁为目标，性能仍必须通过同机 MQTTZero
回归基线和按单元格的 MQTTnet 对比验证。

单进程 100,000 连接容量验证暂不纳入本轮验收，但新架构不得重新引入明显的固定内存和全量扫描瓶颈。

## 验收目标

- 普通发布门禁要求正确性、资源边界、真实传输、分配哨兵和 Dashboard
  测试全部通过。
- 同机稳定基线中，任何单元格吞吐回退不得超过 `5%`；失败数、排空超时和
  Lease 泄漏必须为 0。
- 性能领先声明按单元格和指标分别成立：三次重复后吞吐中位数高于 MQTTnet，
  且发布调用/ACK p99 不劣于 MQTTnet。
- 开发阶段的普通回归使用两次重复；两轮端到端吞吐离散超过 `10%`，或出现
  失败/未排空时补跑第三轮。p99 在开发阶段只记录，不触发第三轮。
- “所有 QoS、订阅规模和 Payload 全面严格领先 MQTTnet”不再作为普通发布门禁。
- 发布调用/ACK 延迟必须与端到端订阅者投递延迟分开报告。
- 无扩展稳态路径继续保持零托管分配。
- TLS、WebSocket、QoS 0/1/2、持久 Session、Retained、Will、Expiry、Session Takeover 和 MQTT 5 属性语义不回归。
- Kestrel 继续作为默认生产传输。

## 公开 API 与默认值

保持 `IMqttZeroBroker`、现有拦截器、Observer、Store 和 Kestrel 接入语义不变。最终删除内部旧管线，公开变更限制为：

| API | 默认值 | 说明 |
|---|---:|---|
| `MqttZeroOptions.EventLoopShardCount` | `0` | 自动使用 `Environment.ProcessorCount`；允许 `0..256` |
| `MqttZeroOptions.OutboundBatchBytes` | `512 KiB` | 单次 Egress 数据批次字节上限 |
| `MqttZeroOptions.OutboundBatchMessages` | `128` | 单次 Egress 数据批次消息数上限 |
| `MqttZeroOptions.OutboundBatchDuration` | `500 us` | 单次 Egress 数据批次持续时间上限 |

其他约定：

- `MaxBufferedBytes` 必须同时核算 Active 和 Cache Buffer，缓存不得绕过预算。
- 新增内部诊断计数器，记录每 Shard 队列深度、执行耗时、Flush 等待、异常、Broker-owned 内存和活跃连接。
- Shard 是串行所有权域，不绑定操作系统线程或 CPU 亲和性。
- 同一 Shard 永远只有一个 `IThreadPoolWorkItem.Execute` 在执行。

## 实施阶段

### Phase 0：建立可信基线

- [ ] 保留现有 MQTTnet Client 对比矩阵。
- [ ] 新增最小化原始 MQTT 5 负载客户端，用于区分 Broker 成本和 MQTTnet Client 成本。
- [ ] Benchmark 子进程显式配置 Server GC、TieredPGO 和 ThreadPool 参数。
- [ ] 记录 OS、CPU、GC、CPU 时间、Working Set、失败数、队列深度和分阶段耗时。
- [ ] 增加 PR 快速校验和 Windows/Linux 夜间性能任务。
- [ ] 正式性能任务必须独立启动 Broker 进程、完成预热排空、关闭日志并使用固定端口范围。
- [ ] 正式矩阵固定为 QoS `0/1/2`、订阅者 `1/100/1000`、Payload `32/8192/65536`，每次三次重复取中位数。
- [ ] Phase 0 本地开发阶段使用 QoS `0/1`、订阅者 `1/100`、2 秒预热、6 秒测量和 1 次重复。
- [ ] Phase 0 夜间稳定矩阵使用 QoS `0/1/2`、订阅者 `1/100`、5 秒预热、20 秒测量和 3 次重复。
- [ ] `1000` 订阅者场景延后到最终性能门禁恢复。
- [ ] 修改热路径前记录当前分支基线，确保 Build、全部测试、分配哨兵和 Benchmark 可重复运行。

### Phase 1：Event Loop Shard 与工作调度

- [ ] 新增内部 `MqttZeroEventLoopShard`，Shard 数默认使用逻辑处理器数。
- [ ] 仿照 Kestrel `IOQueue` 使用可复用 `IThreadPoolWorkItem`、`ConcurrentQueue<Work>`、CAS `scheduled` 标志和 `ThreadPool.UnsafeQueueUserWorkItem(this, preferLocal: false)`。
- [ ] Work 使用强类型 Kind 加连接、Session、Lease、DeliveryBatch 和 TimerToken 引用，热路径不使用 `Action<object?>` 闭包。
- [ ] 每轮最多执行 1024 个 WorkItem 或 500 us。
- [ ] 超过预算后清除 scheduled，并在队列非空时重新投递自身，防止单个 Shard 长期占用 ThreadPool。
- [ ] Shard 内禁止阻塞和直接 `await`；持久化、拦截器或 Flush 的异步续接只允许 Post 回所属 Shard。
- [ ] WorkItem 异常按连接和 Session 隔离并记录。
- [ ] 停止、队列关闭和遗留 Lease 释放必须有确定顺序。
- [ ] Session 使用 Client ID 的稳定 FNV-1a 哈希固定 Shard。
- [ ] CONNECT 前连接暂由 Ingress Shard 处理，解析 Client ID 后将连接 Actor 转移到 Session Shard。
- [ ] Kestrel `SocketTransportOptions.IOQueueCount` 与 Broker Shard 数对齐，但保持两套调度器职责分离。

### Phase 2：Session-scoped Egress 所有权

> 当前状态：架构闭环已完成。旧 `MqttZeroEgressActor` 已删除，
> `MqttZeroSessionEgress` 成为 Session Pending、Inflight、控制帧顺序、编码写入和
> Flush 生命周期的唯一所有者。统一 Connection Writer、强制公平和 Direct/Backlog
> 实验均未通过性能门禁，生产实现已删除。

- [x] 修正 Benchmark Client、发布调用延迟、端到端投递延迟和测量后排空口径。
- [x] 为 Shard 高/低优先级队列提供调度、背压、停止和诊断能力；强制公平实验不进入生产路径。
- [x] 将 Pending、Inflight、控制包顺序、编码写入和 Flush 收敛到
  `MqttZeroSessionEgress`；旧 Pump/Writer 不作为并行生产路径保留。
- [x] Egress 维护控制帧优先队列和待发送数据状态。
- [x] 控制帧只在完整 MQTT 包边界插入。
- [x] 数据批次同时受字节数、消息数和持续时间限制。
- [x] 存在控制帧时，数据批次在下一包边界结束。
- [x] `FlushAsync` 同步完成时继续发送；未完成时进入单一 `FlushPending` 生命周期。
- [x] 消息从 Session Pending 直接进入发送批次，不增加第二次消息复制或队列交接。
- [x] 保留 `MaxInflight`、Receive Maximum、QoS 1/2 Inflight、DUP、重连恢复、过期和队列驱逐语义。
- [x] 保留 PUBACK 与订阅者网络 Flush 解耦；发布者确认等待持久化入站接受，不等待全部订阅者发送完成。
- [ ] 同机性能矩阵恢复到旧基线 `5%` 内，并定位当前吞吐回退。

> A/B 已完成：首个主要吞吐回退为 `d14f433`。该提交在连接读取线程与 Session
> Shard 之间逐 PUBLISH 创建 `MqttZeroShardInvocation<T>`、TaskCompletionSource
> 和异步续接。后续 `6eca005` 去锁与 `ec8ba3f` 连接循环合并没有复现主要吞吐
> 回退。下一步必须先消除该热路径分配，再恢复性能门禁。

### Phase 3：内存密度与单 Slab Lease

> 当前状态：核心单 Slab、池化队列候选链、惰性 Topic Alias 和固定队列内存移除
> 已合入；下方复选框保留为最初实施记录，不作为当前待办列表。

- [ ] `MqttZeroMessageLease` 改为从 Buffer Pool 租赁单个 Slab。
- [ ] Slab 依次保存 Topic、Properties 和 Payload。
- [ ] 入站数据只复制一次，所有订阅者共享同一引用计数 Slab。
- [ ] 增加每 Shard 小型 Buffer 缓存。
- [ ] Cache 字节永久计入 `MaxBufferedBytes`。
- [ ] 池耗尽时暂停所属连接读取，释放容量后通过 Shard WorkItem 恢复。
- [ ] `MqttZeroMessageQueue` 改为惰性侵入式链表，并增加独立 QoS 0 候选链。
- [ ] 删除每个 Session 预分配的 `MaxMessageQueueLength` 数组。
- [ ] 删除每连接固定 Writer Ring 和 Batch List。
- [ ] Topic Alias 表改为首次使用时分配的小型表。
- [ ] 设计目标保持为空闲连接加 Session 的 Broker-owned 状态低于 `4 KiB`，但不以 100,000 连接压测作为本轮发布门禁。

### Phase 4：Route Epoch 与共享订阅索引

> 当前状态：Route Epoch、预构建 Filter 组、Session 去重和 Shard 感知计划核心路径
> 已合入；下方复选框保留为最初实施记录。后续只有在修正后的基准仍证明路由是
> 主要瓶颈时才继续扩展。

- [ ] 实现不可变 `MqttZeroRouteEpoch`。
- [ ] Topic Segment Trie、常规订阅叶节点和共享订阅叶节点处于同一索引。
- [ ] 叶节点使用持久化 HAMT 保存 Session 到路由项的映射。
- [ ] 订阅、退订和 Session 删除通过路径级 Copy-on-Write 修改 Trie，再以 CAS 发布新 Epoch。
- [ ] 发布线程只读取当前 Epoch。
- [ ] 删除 `Encoding.UTF8.GetString` 聚合键。
- [ ] 删除每消息 `RouteScopePool / ConcurrentQueue`，改用 Shard 私有 Route Buffer。
- [ ] 共享组 Cursor 使用独立稳定注册表，按 `group + filter` 保存。
- [ ] 跨 Epoch 更新保持共享订阅轮询位置。
- [ ] Route Planner 按目标 Session 去重、选择最高 QoS、合并 Subscription Identifier，并按目标 Shard 生成 `DeliveryBatch`。
- [ ] 一条 1000 订阅者消息最多产生每目标 Shard 一个批量 WorkItem，而不是 1000 次跨 Shard 投递。
- [ ] 目标 Shard 内继续保证同一 Session 的投递顺序。
- [ ] Retained 消息增加独立 Topic Trie，使 Subscribe 通过 Topic 树查找匹配 Retained，替代全表扫描。
- [ ] 订阅持久化失败时不发布新 Epoch、不修改可见 Session 状态、不发送 SUBACK。
- [ ] 订阅持久化成功后提交 Session 与 Epoch，再发送 ACK。

### Phase 5：时间轮与生命周期

> 当前状态：已完成。每个 Shard 拥有一个四层、每层 512 槽的侵入式时间轮；全局
> Deadline Queue、全局维护扫描和 `PeriodicTimer` 已删除。

- [x] 每个 Shard 持有分层时间轮。
- [x] 时间轮管理 KeepAlive、Will Delay、Session Expiry、Message Expiry 和 Retained Expiry。
- [x] 到期节点使用版本号和侵入式链表，不创建逐消息 Task 或 Timer。
- [x] 到期槽触发时若 Deadline 已变化，则按新值重新挂入。
- [x] `KeepAliveCheckInterval` 继续作为基础 Tick，默认 `500 ms`。
- [x] 不再遍历全部 Session 或 Retained。
- [x] Broker Stop 停止 Shard ticker、排空 Scheduled 工作并释放 Timer/Lease/Buffer 状态。
- [x] 会话接管、断线、Will 和 Store 失败均通过所属 Session Shard 完成。
- [x] 禁止跨 Shard 直接修改 Session 字段。

### Phase 6：扩展语义与兼容路径

> 当前状态：现有扩展和真实传输语义已通过完整回归。当前阻断点是性能，不是协议或
> 扩展兼容性。

- [x] 扩展调用 Broker API 时不得造成 Shard 自等待死锁。
- [x] 保持拦截器顺序、重入、异常隔离、Store Write-Through 和现有 ACK 时序。
- [x] 无 Store、无 Observer、无拦截器时继续使用专用零分配快路径。
- [x] 有扩展时允许额外对象分配。
- [x] TLS、WebSocket、MQTT 5 属性、QoS 2、Session Takeover、Message Expiry 和 Retained 行为全部回归。

### Phase 7：Kestrel 与 Socket A/B

> 当前状态：已关闭。实验已完成并拒绝。明文 `SocketAsyncEventArgs` 原型吞吐低于
> Kestrel，相关生产代码已删除；Kestrel 保持唯一生产传输。决策记录见
> `perf/baseline/broker-transport-ab-2026-11-25.md`。

- [x] 把 Broker 与传输边界抽象为内部 Transport Connection。
- [x] Kestrel 保持生产默认路径。
- [x] 评估明文 TCP Socket 传输原型。
- [x] Socket 原型不支持 TLS、WebSocket 或公开 NuGet API。
- [x] A/B 覆盖单订阅、100/1000 扇出和 32 B/8 KiB/64 KiB。
- [x] 因没有稳定收益删除原型并保留报告。

### Phase 8：删除旧架构与发布门禁

- [x] 删除 `MqttZeroEgressActor`、全局 Deadline Queue 和仅测试使用的旧出站发送路径。
- [x] 删除 Session `SyncRoot`、Session Mutation Lock 和 Session Mutation Lease。
- [x] 删除临时 Egress、Timer 和 Session-lock 兼容开关。
- [x] 更新架构、性能定位和基准方法文档。
- [x] 普通发布前功能、回归、分配和供应链门禁通过。
- [ ] 同机性能矩阵恢复到旧基线 `5%` 内。
- [ ] 只对实际通过三次中位数验证的单元格和指标发布性能声明。
- [ ] 完成性能调查后才允许合并 `master` 或提升版本。

## 测试计划

### Shard

- [ ] 单连接顺序。
- [ ] 跨 Shard 顺序。
- [ ] 时间预算。
- [ ] 重投递和自排队。
- [ ] 异常隔离。
- [ ] 停止排空。
- [ ] 无 Task 泄漏。

### Egress

- [ ] 控制帧优先。
- [ ] Flush 阻塞恢复。
- [ ] 批次字节、消息数和时间边界。
- [ ] 关闭取消。
- [ ] 超时。
- [ ] QoS 1/2 状态。
- [ ] Lease 释放。

### 路由

- [ ] 并发订阅和退订 CAS。
- [ ] 无全局重建。
- [ ] 共享 Cursor 跨 Epoch。
- [ ] Session 去重。
- [ ] Subscription Identifier 合并。
- [ ] No Local。
- [ ] Retained 匹配。

### 内存

- [ ] 单 Slab。
- [ ] Payload 单次复制。
- [ ] Buffer 预算核算。
- [ ] 缓存不超预算。
- [ ] 池耗尽恢复。
- [ ] 空闲队列不预分配大数组。

### 集成

- [ ] TLS。
- [ ] WebSocket。
- [ ] Session Takeover。
- [ ] QoS 2。
- [ ] Will。
- [ ] Expiry。
- [ ] Store 失败回滚。
- [ ] 异步重入拦截器。
- [ ] Observer 异常隔离。

### 分配

- [ ] 无扩展稳态路径保持 `0 B/op`。
- [ ] Shard 调度、Route Plan 和 Egress 路径不得引入每消息托管分配。

### 性能

- [ ] Windows 本地先执行 12 单元格开发矩阵，夜间执行 18 单元格稳定矩阵，最终门禁恢复 27 单元格矩阵。
- [ ] Linux 本地先执行 12 单元格开发矩阵，夜间执行 18 单元格稳定矩阵，最终门禁恢复 27 单元格矩阵。
- [ ] 开发回归每个单元格两次重复；吞吐离散超限或结果无效时补跑第三轮。
- [ ] 最终性能声明每个单元格三次重复取中位数。
- [ ] 每个单元格相对同机稳定 MQTTZero 基线的吞吐回退不超过 `5%`。
- [ ] 只有满足 `ThroughputRatio > 1.00` 且 `PublishCallP99Ratio <= 1.00`
  的单元格才可声明领先 MQTTnet。
- [ ] 失败操作数为 0。
- [ ] 测量结束后的端到端排空时间、实收数和发布数分别记录。
- [ ] 两个平台使用同一 Benchmark Client、相同 Kestrel 参数、Server GC 和负载窗口。
- [ ] 原始 MQTT Client 结果用于定位瓶颈，不替代 MQTTnet Client 严格门禁。

## 必跑命令

```powershell
dotnet restore MqttZero.slnx --locked-mode
dotnet build MqttZero.slnx -c Release --no-restore
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-build
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release --no-build
./eng/verify-coverage.ps1
./perf/verify-allocation.ps1
```

## 假设与边界

- 项目继续保持 MQTT 5 only 和 `net10.0`。
- 本轮不新增集群、桥接、客户端 SDK 或数据库实现。
- 外部 Store 继续负责持久化，Broker 不改变其线程安全与复制语义。
- Kestrel 是默认生产传输。
- Socket 实验已关闭，不进入生产路径。
- 单进程 100,000 连接容量验证延后。
- 重构期间允许多个阶段性内部开关。
- 最终合并状态只保留一套 Broker 调度实现和一个永久 Kestrel 兼容传输。

