# MQTTZero Phase 4.6 Architecture and Performance Review

日期：2026-11-25

分支：`perf/outperform-mqttnet`

## 结论

当前性能上不去，主要不是某个算法还不够快，而是三层设计共同放大了每次发布的
延迟：

1. 入站 PUBLISH 被提升为 owned Slab，随后网络发送又复制一次 Payload。
2. 每条消息进入 Session Pending，再产生 Egress Signal、Actor Wake、Batch、
   FlushWait 生命周期。
3. 大包批次实际平均只有约 `1.2` 条消息，几乎所有批次都会落到异步 Flush
   等待，随后再由回调重新调度。

编码、Topic Matcher 和 Route Planner 的微基准并没有证明是当前主要瓶颈。
继续只改 `Encode` 或批次常量，预计收益有限。

**方向判断：Phase 5 不能启动。应先重构 Session 到 Socket 的数据流，并修正
基准客户端，再判断是否需要新的路由或运行时模型。**

## 实测证据

### QoS 0 / 100 订阅者 / 64 KiB Outbound

| 指标 | 数值 |
|---|---:|
| 发布数 | 1,280 |
| 投递数 | 128,000 |
| 编码消息 | 127,193 |
| Egress Signal | 128,000 |
| Egress Drain | 107,884 |
| Data Batch | 107,884 |
| Flush Wait | 107,884 |
| 平均消息/批次 | 1.179 |
| 平均 Fanout | 2,504.9 us |
| 最大 Fanout | 16,699.1 us |
| Publish p50 | 6,432.9 us |
| Publish p99 | 15,454.2 us |

### QoS 1 / 100 订阅者 / 64 KiB Outbound

| 指标 | 数值 |
|---|---:|
| 发布数 | 1,135 |
| 投递数 | 113,500 |
| 编码消息 | 112,639 |
| Egress Signal | 226,077 |
| Egress Drain | 107,057 |
| Data Batch | 92,948 |
| Flush Wait | 92,947 |
| 平均消息/批次 | 1.212 |
| 平均 Fanout | 2,477.7 us |
| 最大 Fanout | 27,624.2 us |
| Publish p50 | 7,130.9 us |
| Publish p99 | 18,060.9 us |

### Raw Client 对照

使用 Raw MQTT Client 排除 MQTTnet 客户端每次 `payload.ToArray()` 的影响：

| 场景 | MQTTZero/MQTTnet 吞吐 | p99 比 |
|---|---:|---:|
| QoS 0 / 100 / 64 KiB | 0.89x | 1.18x |
| QoS 1 / 100 / 64 KiB | 0.97x | 0.74x |

Raw Client 下差距明显缩小，说明 MQTTnet Benchmark Client 自身分配会放大差距，
但 MQTTZero 仍有真实发送链路瓶颈。

## 根因

### 1. 发送批次没有形成有效合批

100 个订阅者广播会造成 100 个独立 Session Egress。每个 Session 的队列通常
只有少量消息，因此平均 Batch 约 `1.2` 条。

结果是：

- 每次 Batch 写少量 64 KiB PUBLISH。
- 几乎每次 Batch 都需要异步 Flush。
- 每次 Flush 完成都要一次 Egress Scheduler 唤醒。
- Shard 上形成大量短生命周期 Work。

大包场景中，Shard 调度和 PipeWriter Flush 延迟直接叠加到 Publish p99。

### 2. QoS 0/1 数据仍有两次 Payload 流量

入站 `BorrowFrom()` 之后：

- FastPath 调用 `EnsureOwned()`，复制 Topic、Properties、Payload 到 Slab。
- 发送时 `MqttPublishPacketCodec.Encode` 或 `EncodeToWriter` 再将 Payload
  写入目标 PipeWriter。
- 64 KiB 时一次发布至少产生读取输入、写入 Slab、写入输出三条内存路径。

因此“0 B/op”并不等于零复制或低内存带宽。池化只消除了 GC 分配，没有消除
大包复制和陈旧 Slab 的生命周期成本。

### 3. 调度器分层过多

当前一条消息的对象与调度生命周期为：

```text
Connection Read
  -> Message Lease
  -> Fanout Operation
  -> Fanout Scheduled Batch
  -> Target Shard Work
  -> Session Queued Message
  -> Egress Ready Actor
  -> Egress Drain
  -> Data Batch
  -> Pipe Write
  -> Flush Pending
  -> Flush Completion
  -> Egress Wake
  -> next batch
```

零分配通过对象池实现，但这些对象的 Refcount、CAS、Queue、Signal、Task Source
和锁仍然是实打实的 CPU 路径。线程数越多，跨 Shard 唤醒与空转越明显。

### 4. Session 锁覆盖了发送批次选择

`DrainQueuedMessagesOnceAsync` 在 `session.SyncRoot` 内完成：

- Pending Expiry 检查。
- Inflight Limit 检查。
- 多条消息 Dequeue。
- Packet Identifier 分配和 Inflight 字典插入。
- Encoded Length 校验。

100 个 Session 在不同 Shard 上并行时，单个 Session 的锁本身通常不是热点，
但它使“选择批次”和“开始写 Pipe”绑定在同一个 Shard 执行单元里。发送 I/O
等待期间，后续消息只能在 Pending 中积累。

### 5. 两套调度器的优先级不一致

- Fanout Scheduler：高优先级。
- Egress Scheduler：低优先级。

发布者路由可以快速推进，但目标 Session 的实际发送只能等低优先级 Egress。
这解释了为什么 ACK 可能已优化，但投递和 p99 仍落后。

### 6. Benchmark 客户端污染了比较

`MqttNetBenchmarkMqttClient.PublishAsync` 每次执行：

```csharp
.WithPayload(payload.ToArray())
```

64 KiB 每发布一次都会分配一个新的 64 KiB Payload 数组。两侧 Broker 都使用
同一个客户端，因此差距不会完全失真，但客户端 GC、复制和 Latency 会混入结果。

这不是 Broker 的借口；它让当前严格矩阵缺少足够的归因能力。

### 7. 路由规模仍有真实缺口

当前 Route Planner 会在发布线程按目标 Shard 生成计划，并展开匹配订阅。
100 订阅者场景下它不是唯一瓶颈，但 500 订阅者时仍会被 Fanout 和 Egress
低效放大。继续用订阅者数量而不是“每目标 Shard 的 Batch 数和消息数”衡量
会掩盖真实成本。

## 方向判断

### 优先级 A：重构 Session 发送流

不要继续微调 `Encode`、Matcher 常量或 Batch 大小。目标应是：

- 每条连接的发送状态机统一处理 Data 和 Control。
- 一个 Actor 在 FlushPending 期间不被重新调度。
- Flush 完成后续排不使用额外的常驻唤醒链。
- 批次在队列中积累到合适边界，而不是每次仅有 `1.2` 条就 Flush。
- 低流量立即 Flush；积压时允许小规模合批。

这可能需要从现有“每次 Acknowledge/Flush 都 Signal Scheduler”改为
“连接 Writer 自身持有待发送批次”的长期状态机。

### 优先级 B：区分 QoS 0 单播与多播的所有权

单播 QoS 0 最值得实验“借用 + 同步写 Pipe + 完成后释放输入 Ownership”：

- 路由只有一个 Receiver。
- 没有 Retained、Persistence、Observer、Interceptor。
- 连接 Writer 在发布者 `AdvanceTo` 前完成写入。

多播、QoS 1/2、离线、Retained 仍提升一次并共享 Lease。

如果必须在 `AdvanceTo` 后继续持有 Payload，就不应宣称单播“零中间复制”；
所有权必须在代码和测试中明确。

### 优先级 C：修正基准

必须增加：

- 不带 `ToArray()` 的 MQTTnet Client Benchmark Adapter。
- Raw Client 作为归因诊断，不作为最终门禁。
- 记录 Broker 入站、Fanout、Session Enqueue、Egress、Pipe Write、Flush
  的独立等待和耗时。
- 每目标 Shard Batch 数、平均 Batch 消息数和 Flush Wait 比率。

### 优先级 D：再处理路由和 Session 锁

只有在发送链路优化后仍然落后，才继续做 Session 锁分段和无锁 Batch 选择。
当前没有证据证明 Session 锁是第一瓶颈。

### 优先级 E：拓扑与 GC 优化

`1 Shard + 1 IOQueue` 能改善单订阅 p99，但不能作为默认；需要先修复大包排空。
GC 和 ThreadPool 调参不能替代发送状态机重构。

## 明确不建议

- 不继续把完整 64 KiB 报文复制到一个新数组再交给 PipeWriter。
- 不为单订阅强行拼接 Header+Payload。
- 不把低优先级 Egress 随便升为高优先级而不测 100/500 扇出。
- 不用增加等待时间或放宽 Draining 超时掩盖停顿。
- 不在没有 Raw/Ack/Flush 分阶段数据时继续改 Batch 常量。

## 后续计划建议

1. 修复 Benchmark Client 的 Payload 复制，重新建立同机基线。
2. 把 Egress Data 与 Control 合并为一个长期 Writer 状态机，只保留一个
   FlushPending，不再用每次 Flush 重新唤醒 Actor 形成链式调度。
3. 为 QoS 0 单播增加“无中间 Slab”实验，严格控制所有权边界。
4. 完成 1/100/500 的 32 B/8 KiB/64 KiB 三次矩阵。
5. 只有发送链路后仍落后时，再做 Route Trie、Session 锁和拓扑收敛。

## 最终判断

当前问题的性质是：

```text
50% 调度/发送状态机设计
30% 大包复制与内存带宽
15% Benchmark 客户端和归因缺口
5% 局部编码/路由微优化
```

这不是“再调一点参数”的问题，而是一次发送热路径架构收尾。
在 Writer 状态机重构前，不应该继续宣称零分配等于高性能，也不应该合并
到 `master`。
