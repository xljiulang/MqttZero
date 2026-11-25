# Phase 4.6 Egress Shard Batching Baseline

日期：2026-11-25

## 结论

**Phase 4.6 保持红色。不得启动 Phase 5。**

本轮完成了 Phase 4.6 的网络稳定性修复，12 格三次矩阵可以完整运行，
不再因 `QoS 0 / 100 / 64 KiB` 漏唤醒而超时。但严格 MQTTnet 门禁只通过
`2/12`，同机 Phase 0 开发回归只通过约 `5/12`，因此不能宣布阶段性达标。

## 环境

- Windows `10.0.26300`
- Intel Core i5-12450H，8 物理核 / 12 逻辑核
- .NET SDK `10.0.400`
- .NET Runtime `10.0.15`
- Server GC、Concurrent GC、TieredPGO 启用
- 默认 Event Loop Shards：`12`
- 默认 Kestrel IOQueue：`12`
- MQTTnet `5.2.0.1603`
- 客户端：仅 MQTTnet Client

## 已修复问题

### 控制包与数据 Flush 竞态

- 数据 `FlushPending` 时，当前 Shard 的控制包不再直接写同一个
  `PipeWriter`。
- 控制帧转交给 Egress Actor，调用方立即返回；Actor 在数据 Flush
  完成后写入控制帧。
- 只有 deferred 控制帧由 Actor 释放，普通等待路径仍由调用方释放，
  避免完成状态尚未读取时池化帧被复用。

### 异步 Flush 后的批次续排

- 异步 Flush 分支现在返回真实 Pending 状态。
- Actor 在 Flush 完成前保存续排信号，Flush 完成后继续下一批。
- 队列耗尽时不再无条件执行空 Drain。

### Flush 期间的新数据信号

- 修复 `SignalCore()` 将 `flushPending` 放入早退条件的问题。
- 新数据在 FlushPending 时设置 `dataSignaled`，Flush 完成后重新调度 Actor。
- Dump 中“101 个连接在线、所有 Actor Idle、20 个 Session 仍有 24 条 Pending”
  的漏信号现场已由确定性测试覆盖。

### Benchmark 诊断

- 默认订阅者矩阵上限从 `1000` 收敛到用户要求的 `1/100/500`。
- 排空超时现在报告 repetition、Broker、QoS、订阅者、Payload、端口、
  已发布、应收和实收数量。

## 验证

```text
Core tests:                  574/574 passed
Broker benchmark tests:       29/29 passed
Allocation sentinels:         21/21 passed
Release build:                0 warnings, 0 errors
dotnet format --verify:       passed
git diff --check:             passed
```

关键新增测试：

- `SignalWhileFlushPendingResumesAfterCompletion`
- `ControlWaitsForPendingDataFlush`
- `SignalDuringDrainWaitsForPendingFlush`
- `CompletedFlushWithoutWorkDoesNotResumeActor`
- `PendingMessagesResumeAfterBatchLimitedAsyncFlush`

## 正式 12 格矩阵

命令：

```powershell
dotnet artifacts/phase4.6/broker-bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet `
  --qos 0,1 `
  --subscribers 1,100 `
  --payload 32,8192,65536 `
  --warmup 2 `
  --seconds 6 `
  --repetitions 3 `
  --max-pending 1024 `
  --start-port 58400 `
  --output artifacts/phase4.6/phase4.6-final-3x-v2.json
```

原始 JSON：`broker-phase4.6-2026-11-25.json`

| QoS | 订阅者 | Payload | MQTTZero msg/s | MQTTnet msg/s | 吞吐比 | MQTTZero p99 us | MQTTnet p99 us | p99 比 | 结果 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| 0 | 1 | 32 | 22,575.23 | 22,387.97 | 1.01x | 85.5 | 87.8 | 0.97x | PASS |
| 0 | 1 | 8 KiB | 21,250.86 | 24,239.08 | 0.88x | 123.2 | 99.7 | 1.24x | FAIL |
| 0 | 1 | 64 KiB | 3,593.08 | 12,465.42 | 0.29x | 8,320.5 | 331.2 | 25.12x | FAIL |
| 0 | 100 | 32 | 8,853.32 | 4,995.49 | 1.77x | 201.3 | 380.4 | 0.53x | PASS |
| 0 | 100 | 8 KiB | 1,238.33 | 1,511.92 | 0.82x | 8,237.5 | 12,402.9 | 0.66x | FAIL |
| 0 | 100 | 64 KiB | 152.78 | 600.12 | 0.25x | 118,587.2 | 31,531.6 | 3.76x | FAIL |
| 1 | 1 | 32 | 7,808.11 | 9,108.26 | 0.86x | 253.4 | 219.5 | 1.15x | FAIL |
| 1 | 1 | 8 KiB | 6,627.97 | 7,235.83 | 0.92x | 314.4 | 342.8 | 0.92x | FAIL |
| 1 | 1 | 64 KiB | 2,090.37 | 4,581.00 | 0.46x | 15,004.9 | 646.8 | 23.20x | FAIL |
| 1 | 100 | 32 | 683.10 | 701.78 | 0.97x | 2,426.4 | 3,517.7 | 0.69x | FAIL |
| 1 | 100 | 8 KiB | 412.32 | 454.83 | 0.91x | 4,728.0 | 5,352.0 | 0.88x | FAIL |
| 1 | 100 | 64 KiB | 107.29 | 114.01 | 0.94x | 18,233.6 | 20,435.3 | 0.89x | FAIL |

严格门禁：`2/12 PASS`。

## 同机 Phase 0 对照

旧 Phase 0 和 Phase 4.5 基线来自 i7-8565U / 8 逻辑核，不能直接用于当前
i5-12450H 的 5% 回归判定。本轮额外检出 `44d1654`，在同一机器重建 Phase 0。

同机 Phase 0 JSON：`broker-phase0-same-machine-2026-11-25.json`

| 单元 | 吞吐比 | p99 比 | 结果 |
|---|---:|---:|:---:|
| 0/1/32 | 1.07x | 1.00x | PASS |
| 0/1/8 KiB | 1.00x | 1.13x | FAIL |
| 0/1/64 KiB | 0.89x | 2.47x | FAIL |
| 0/100/32 | 1.01x | 1.05x | PASS |
| 0/100/8 KiB | 1.21x | 0.69x | PASS |
| 0/100/64 KiB | 1.06x | 0.98x | PASS |
| 1/1/32 | 0.84x | 1.30x | FAIL |
| 1/1/8 KiB | 0.84x | 1.23x | FAIL |
| 1/1/64 KiB | 0.57x | 11.27x | FAIL |
| 1/100/32 | 0.93x | 0.88x | FAIL |
| 1/100/8 KiB | 0.89x | 0.91x | FAIL |
| 1/100/64 KiB | 0.96x | 0.90x | PASS |

开发回归：约 `5/12 PASS`。

## 批次 A/B

固定 `QoS 1 / 1 订阅者 / 64 KiB`：

| Batch Bytes | 吞吐比 | p99 比 |
|---:|---:|---:|
| 512 KiB（默认） | 约 0.53x | 约 21x |
| 256 KiB | 0.41x | 24.50x |
| 128 KiB | 0.38x | 24.17x |
| 1 MiB | 0.51x | 21.22x |
| fragmentation=false | 0.22x | 21.27x |

默认 512 KiB 已是当前最佳配置，继续缩小或放大都会退化。

## 拓扑证据

`QoS 1 / 1 订阅者 / 64 KiB` 下单次 A/B：

| Event Shards | IOQueue | 吞吐 | p99 us |
|---:|---:|---:|---:|
| 12 | 12 | 约 2.4k msg/s | 约 13.6k |
| 12 | 1 | 约 2.4k msg/s | 约 13.6k |
| 1 | 12 | 约 2.4k msg/s | 约 13.9k |
| 1 | 1 | 约 3.06k msg/s | 约 3.48k |

只有 `1 Shard + 1 IOQueue` 明显降低单订阅尾延迟，但会破坏 100 订阅者扇出
并触发排空超时，因此不能作为默认生产拓扑。

## 当前瓶颈

- 跨 Shard 路由与 Egress 唤醒仍是单订阅 64 KiB 尾延迟的主要来源。
- Broker 在高吞吐 64 KiB 发送中仍有约 `120–268 MB/s` 的托管分配速率；
  同机 Phase 0 也有相近分配，GC 是尾延迟放大器但不是唯一原因。
- 100 订阅者 QoS 0 大包受 Kestrel 输出背压和 Egress 批次限制。
- `1 Shard` 可降低单订阅尾延迟，但当前多连接扇出在该拓扑下不稳定。

## QoS 1 调度后 ACK 跟进

已将 QoS 1 FastPath 和可调度的 Core Publish 路径改为：

1. 完成 Publish Interceptor 和必要持久化。
2. 将每个目标 Shard 的非空 Fanout Batch 写入有界队列。
3. 所有 Batch 入队成功后发送 PUBACK。
4. 实际 Session 路由、Enqueue Interceptor 和 Delivery Observer 由目标
   Shard 后台完成。

启用 Message Store 时继续等待 Fanout 完成，保留持久化 ACK 屏障。
QoS 2 和管理面发布保持原等待语义。

单订阅 `QoS 1 / 64 KiB` 三次结果：

| 状态 | MQTTZero msg/s | p99 us | 相对 MQTTnet 吞吐 | 相对 MQTTnet p99 |
|---|---:|---:|---:|---:|
| 调度后 ACK 前 | 约 2,090 | 15,005 | 0.46x | 23.20x |
| 调度后 ACK 后 | 2,548 | 11,574 | 0.60x | 16.40x |

提升约：

- 吞吐：`+22%`
- p99：改善约 `23%`

100 订阅者三次结果：

| Payload | MQTTZero msg/s | MQTTnet msg/s | 吞吐比 | MQTTZero p99 us | p99 比 |
|---:|---:|---:|---:|---:|---:|
| 32 B | 624.33 | 694.84 | 0.90x | 2,567.2 | 0.72x |
| 8 KiB | 397.74 | 444.59 | 0.89x | 5,385.7 | 0.99x |
| 64 KiB | 105.56 | 109.72 | 0.96x | 15,885.5 | 0.78x |

调度后 ACK 显著降低单播大包延迟，但不能解决 64 KiB 输出复制和跨 Shard
吞吐上限；严格门禁仍未通过。

原始结果：

- `broker-phase4.6-scheduled-ack-q1-single-2026-11-25.json`
- `broker-phase4.6-scheduled-ack-q1-100-2026-11-25.json`

## 持久 Fanout Scheduler 跟进

每个 Event Loop Shard 现在拥有一个持久 Fanout Scheduler：

- 每个目标 Shard 的单次发布 Batch 进入有界 FIFO。
- 多个发布批次共享一个高优先级 Shard Work。
- 单次执行最多 `250 us`、`64` 个批次，避免长时间占用 Shard。
- 同步容量快路径保持 `0 B/op`；队列满时才进入异步背压。
- Scheduler Stop 先排空已接受批次，再由 ShardSet 停止 Event Loop。

QoS 1 / 100 订阅者三次中位数：

| Payload | MQTTZero msg/s | MQTTnet msg/s | 吞吐比 | MQTTZero p99 us | p99 比 |
|---:|---:|---:|---:|---:|---:|
| 32 B | 659.81 | 660.38 | 1.00x | 3,730.4 | 1.00x |
| 8 KiB | 403.05 | 438.11 | 0.92x | 5,228.4 | 0.87x |
| 64 KiB | 102.25 | 108.47 | 0.94x | 21,246.1 | 0.95x |

32 B 保持吞吐，8/64 KiB 的吞吐回退未超过 Task 2 的5%门禁，
8/64 KiB 的 p99 均优于 MQTTnet。

## 大包写入实验结论

尝试将单 Segment `>= 64 KiB` 的 Payload 改为直接
`PipeWriter.WriteAsync(ReadOnlyMemory<byte>)`。

测试证明字节等价且只需一次 Flush，但三次基准结果明显恶化：

| QoS | 订阅者 | 吞吐比 | p99 比 |
|---:|---:|---:|---:|
| 0 | 1 | 0.36x | 17.87x |
| 0 | 100 | 0.55x | 1.97x |
| 1 | 1 | 0.23x | 19.92x |
| 1 | 100 | 0.85x | 1.06x |

该实验已从生产路径和测试中删除。大包尾延迟不能通过把 Payload 单独交给
Kestrel `WriteAsync` 解决。

## 后台 Fanout 故障

Enqueue Interceptor 等后台投递阶段的永久失败现在会：

- 记录源生成日志和失败计数。
- 立即退出 `Running`，使 Readiness 不健康。
- 发起一次性 Broker Stop，拒绝新连接并清理已接受资源。

Delivery Observer 异常仍按现有扩展语义记录后隔离，不会停止 Broker。

## 500 订阅者快速矩阵

单次开发矩阵：

| QoS | Payload | MQTTZero msg/s | MQTTnet msg/s | 吞吐比 | MQTTZero p99 us | MQTTnet p99 us |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 32 B | 3,548.92 | 1,666.74 | 2.13x | 175.6 | 386.0 |
| 0 | 8 KiB | 170.99 | 254.09 | 0.67x | 43,883.2 | 96,931.9 |
| 0 | 64 KiB | 25.41 | 117.72 | 0.22x | 670,611.7 | 123,142.1 |
| 1 | 32 B | 106.98 | 133.40 | 0.80x | 42,716.9 | 14,800.2 |
| 1 | 8 KiB | 76.04 | 75.72 | 1.00x | 23,386.9 | 32,144.7 |
| 1 | 64 KiB | 17.29 | 19.71 | 0.88x | 79,511.3 | 96,202.5 |

没有连接失败或排空超时，但严格门禁仍为红色。

稳定性测试：

- `ScheduledFanoutRepeatedCyclesReleaseLeases` 连续 1,000 次调度、
  完成和 Lease 释放后 BufferPool 为0。
- 现有 Message Store ACK 屏障、QoS 1 DUP 重连和队列淘汰测试通过。

## 下一步

1. 继续优化跨 Shard Fanout 唤醒延迟，优先复用每个目标 Shard 的持久
   fanout work，而不是每发布创建一个 Operation/Work 生命周期。
2. 诊断 `1 Shard` 下 100 订阅者排空超时，只有修复后才能评估低 Shard 默认值。
3. 对 64 KiB 输出使用可证明不增加尾延迟的 PipeWriter 写入策略。
4. 重新建立同机 Phase 0 三次基线，再执行严格 `12/12` 门禁。

Phase 5 继续阻塞。
