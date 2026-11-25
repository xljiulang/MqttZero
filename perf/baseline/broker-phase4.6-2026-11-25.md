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

## 下一步

1. 继续优化跨 Shard Fanout 唤醒延迟，优先复用每个目标 Shard 的持久
   fanout work，而不是每发布创建一个 Operation/Work 生命周期。
2. 诊断 `1 Shard` 下 100 订阅者排空超时，只有修复后才能评估低 Shard 默认值。
3. 对 64 KiB 输出使用可证明不增加尾延迟的 PipeWriter 写入策略。
4. 重新建立同机 Phase 0 三次基线，再执行严格 `12/12` 门禁。

Phase 5 继续阻塞。
