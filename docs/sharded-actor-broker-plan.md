# MQTTZero Sharded Actor Broker 性能重构计划

## 状态

- 状态：待执行
- 目标分支：`perf/outperform-mqttnet`
- 目标框架：`net10.0`
- 记录日期：2026-11-25

## 背景与目标

当前实现的编解码、消息 Lease、常规 Topic Trie 和稳态零分配路径已经具备较好基础，但 Broker 端到端架构仍存在以下瓶颈：

- 每个在线 Session 同时维护 Outbound Pump 和 Connection Writer 两级异步调度。
- 单发布者通常固定在一个 Inbound Dispatcher Worker 中，无法利用全部核心完成扇出。
- 订阅变更会遍历并重建全部 Session 路由。
- 共享订阅和 Retained 订阅匹配仍包含线性扫描。
- 每个 Session 和连接存在与流量无关的固定大数组分配。
- KeepAlive、Will、Session 和 Retained 清理每 500 ms 全量扫描。

本计划将 Broker 重构为“固定 Shard 所有权 + 单连接 Egress Actor + 不可变路由快照”的架构，并通过与 MQTTnet 的同机严格对比验证性能提升。

单进程 100,000 连接容量验证暂不纳入本轮验收，但新架构不得重新引入明显的固定内存和全量扫描瓶颈。

## 验收目标

- Windows 和 Linux 均完成正式 Broker 对比矩阵。
- 正式矩阵中的每个单元格都必须同时满足：
  - MQTTZero 吞吐中位数严格高于 MQTTnet。
  - MQTTZero p99 中位数严格低于 MQTTnet。
  - 失败操作数为 0。
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

### Phase 2：单连接 Egress Actor

- [ ] 合并 Session Outbound Pump、Connection Writer 和连接写锁为 Session 所属 Shard 中的唯一 Egress 状态机。
- [ ] Egress 维护控制帧优先队列和待发送数据状态。
- [ ] 控制帧只在完整 MQTT 包边界插入。
- [ ] 数据批次同时受字节数、消息数和持续时间限制。
- [ ] 存在控制帧时，数据批次在下一包边界结束。
- [ ] `FlushAsync` 同步完成时继续发送。
- [ ] `FlushAsync` 未完成时设置 `flushPending`，注册完成回调并 Post 回同一 Shard。
- [ ] 不创建每连接 Task 或常驻异步循环。
- [ ] 删除连接向 Egress 的第二次消息复制和第二次队列交接。
- [ ] 消息从 Session Pending 直接进入发送批次。
- [ ] 保留 `MaxInflight`、Receive Maximum、QoS 1/2 Inflight、DUP、重连恢复、过期和队列驱逐语义。
- [ ] 保留 PUBACK 与订阅者网络 Flush 解耦；发布者确认等待持久化入站接受，不等待全部订阅者发送完成。

### Phase 3：内存密度与单 Slab Lease

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

- [ ] 每个 Shard 持有分层时间轮。
- [ ] 时间轮管理 KeepAlive、Will Delay、Session Expiry、Message Expiry 和 Retained Expiry。
- [ ] 每个 Session 保存 Deadline 原子字段和 Timer Version。
- [ ] 收到数据只更新 Deadline，不立即重排 Timer。
- [ ] 到期槽触发时若 Deadline 已变化，则按新值重新挂入。
- [ ] `KeepAliveCheckInterval` 继续作为基础 Tick，默认 `500 ms`。
- [ ] 不再遍历全部 Session 或 Retained。
- [ ] Broker Stop 按“停止接收新连接 -> 排空 Scheduled 工作 -> 关闭 Actor -> 释放 Lease/Buffer/Store 状态”的顺序停止全部 Shard。
- [ ] 会话接管、断线、Will 和 Store 失败均通过 Post 回 Session Shard 完成。
- [ ] 禁止跨 Shard 直接修改 Session 字段。

### Phase 6：扩展语义与兼容路径

- [ ] 异步拦截器和 Observer 在 Shard 外执行，完成后将结果 Post 回原 Shard。
- [ ] 扩展调用 Broker API 时不得造成 Shard 自等待死锁。
- [ ] 保持拦截器顺序、重入、异常隔离、Store Write-Through 和现有 ACK 时序。
- [ ] 无 Store、无 Observer、无拦截器时继续使用专用零分配快路径。
- [ ] 有扩展时允许额外对象分配。
- [ ] TLS、WebSocket、MQTT 5 属性、QoS 2、Session Takeover、Message Expiry 和 Retained 行为全部回归。

### Phase 7：Kestrel 与 Socket A/B

- [ ] 把 Broker 与传输边界抽象为内部 Transport Connection。
- [ ] Kestrel 保持生产默认路径。
- [ ] 在 Benchmark 或实验程序集内增加 Windows/Linux 同权的明文 TCP Socket 传输原型。
- [ ] Socket 原型使用 `SocketAsyncEventArgs`、Pooled Pipe Buffer 和 Shard Post 处理完成事件。
- [ ] Socket 原型不支持 TLS、WebSocket 或公开 NuGet API。
- [ ] A/B 覆盖单订阅、100/1000 扇出和 32 B/8 KiB/64 KiB。
- [ ] 若 Socket 原型没有稳定收益，保留报告并删除原型。

### Phase 8：删除旧架构与发布门禁

- [ ] 删除 `MqttZeroInboundDispatcher`。
- [ ] 删除 `MqttZeroSessionOutboundPump`。
- [ ] 删除 `MqttZeroConnectionWriter`。
- [ ] 删除旧 Route Snapshot Builder。
- [ ] 删除临时兼容开关。
- [ ] 更新 README、配置文档、性能基线和架构说明。
- [ ] 只有全部功能测试和严格性能门禁通过后才提升版本。
- [ ] 任何单元格失败都不得发布性能声明。

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
- [ ] 每个单元格三次重复取中位数。
- [ ] 每个单元格满足 `ThroughputRatio > 1.00`。
- [ ] 每个单元格满足 `P99Ratio < 1.00`。
- [ ] 失败操作数为 0。
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
- Socket 首轮仅验证明文 TCP 数据面。
- 单进程 100,000 连接容量验证延后。
- 重构期间允许多个阶段性内部开关。
- 最终合并状态只保留一套 Broker 调度实现和一个永久 Kestrel 兼容传输。

