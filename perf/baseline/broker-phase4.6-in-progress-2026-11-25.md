# Phase 4.6 Egress Shard Batching In-Progress Record

日期：2026-11-25

## 状态

**未完成，性能门禁为红色。不要启动 Phase 5。**

> 后续更新：`QoS 0 / 100 / 64 KiB` 的排空超时已经在同一工作分支中定位并修复。
> 根因是 `SignalCore()` 在 `FlushPending` 时提前返回，没有设置
> `dataSignaled`。修复后 12 格三次矩阵可以完整运行，但严格 MQTTnet
> 门禁仍只通过 `2/12`。最终结果见
> `broker-phase4.6-2026-11-25.md`。

已实现并通过局部测试的改动：

- 每个 Event Loop Shard 一个低优先级 Egress Scheduler。
- ready Actor 去重和 Shard 内批量调度。
- 现有 `OutboundBatchBytes`、`OutboundBatchMessages`、
  `OutboundBatchDuration` 已进入真实批次选择。
- Egress Scheduler、Drain、Data Batch、Flush 和编码字节诊断。
- Benchmark 参数与诊断计数器重置。

## 当前提交

```text
25806d9 perf: honor outbound batch limits
36eecef perf: schedule egress per shard
90da374 perf: instrument egress batch scheduling
```

## 已验证项目

以下测试在该阶段修改期间通过：

- Egress Actor、Scheduler、Flush、ShardSet、OutboundDelivery 相关测试。
- QoS 0/1/2 与 Broker Lifecycle 相关测试。
- Batch budget、消息数、编码字节和时限边界测试。
- 分配哨兵：`21/21` 通过。
- Solution Build：零警告、零错误。

合并运行生命周期测试时出现过一次
`ConcurrentStopAndDisposeDrainConnectedClient` 的
`ObjectDisposedException: CancellationTokenSource has been disposed`。
该测试单独重跑通过，目前标记为需要继续观察的并发停止风险，不能视为已解决。

## 核心诊断结果

当前分支一次重建后的 `QoS 0 / 100 订阅者 / 8 KiB` Outbound 测量：

| 指标 | 数值 |
|---|---:|
| 吞吐 | 208.88 msg/s |
| Publish p99 | 12,975.4 us |
| 编码消息 | 125,400 |
| Actor Drain | 108,158 |
| Drain / Encoded | 0.863 |
| Scheduler Pass | 9,475 |
| 去重后 Actor 调度 | 108,158 |
| Data Batch | 108,107 |
| 平均消息/批次 | 1.16 |
| Flush Start | 108,107 |
| Flush Wait | 947 |
| 平均 Actor Drain | 12.82 us |
| 最大 Actor Drain | 158,397.5 us |

决策门要求 Actor Drain 相对编码消息下降至少 `30%`。实际 Drain 比只有
`0.863`，相对旧诊断约 `0.869` 几乎没有改善，因此没有继续做四组参数调优。

Scheduler 将调度 pass 从旧诊断的十万级降到约 `9.5k`，但每个 ready Actor
仍通常只有一条积压消息。共享调度没有形成跨发布的数据合批。

## 未解决的网络稳定性问题

第一轮完整 12 格矩阵在预热排空阶段失败。后续将矩阵逐格复现后，问题集中在：

```text
QoS 0 / 100 subscribers / 64 KiB
```

短跑 `warmup=1s`、`measurement=2s` 连续 5 次的结果：

- 4 次在 `BenchmarkLoadRunner.WaitForSubscriberDrainAsync` 超时后进程异常退出。
- 1 次完成，MQTTZero 为 `62.02 msg/s`、p99 `268,230.2 us`、失败数 `0`。
- 完成样本的 warmup 发布 `51` 条，订阅端收到 `5,100` 条，说明该次数据最终排空。

一次成功但高抖动的对照结果：

```text
mqttzero: 62.02 msg/s, p99 268230.2 us
mqttnet: 318.99 msg/s, p99 52584.1 us
```

### Dump 证据

已采集：

- `artifacts/phase4.6/server.dmp`
- `artifacts/phase4.6/parent.dmp`

父进程线程停在 `WaitForSubscriberDrainAsync`。服务器 dump 中所有受管线程池线程
空闲，没有正在执行的 Egress/Scheduler 栈。当前只能确定处理流程已经停止推进，
尚未区分以下根因：

1. Scheduler ready 队列丢失 Actor 唤醒；
2. Actor 已进入 FlushPending，但输出 Flush 不再完成；
3. 某些订阅 Session 的 Pending 队列仍有数据但 Egress 未再次调度；
4. MQTTnet 客户端读取侧停滞，而 Broker 已尝试发送。

旧版 WinDbg 自带 SOS 与 .NET 10 运行时不兼容，`cdb .loadby sos coreclr`
无法加载；`dotnet-dump` 可读取线程，但大堆分析很慢。

## 下一台机器的继续步骤

1. 在任何修改前重新构建：

```powershell
dotnet build MqttZero.slnx -c Release --no-restore
```

2. 复现最短失败单元：

```powershell
dotnet run -c Release --no-build --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --client mqttnet --qos 0 --subscribers 100 --payload 65536 `
  --warmup 1 --seconds 2 --repetitions 1 --max-pending 1024 `
  --start-port 68000 --output artifacts/phase4.6/repro.json
```

3. 在失败进程存活时抓 dump，并重点检查：

- `MqttZeroEgressScheduler._ready`
- `_scheduled`、`_activeSignals`
- 每个 Actor 的 `_ready`、`_executing`、`_flushPending`、
  `_dataSignaled`、`_state`
- Session `PendingMessages.Count`
- Kestrel output Pipe 的 Flush 等待状态

4. 第一优先级测试假设：

```text
Actor 在 flushPending 状态收到新消息后只设置 dataSignaled；
如果 Flush 完成与 Signal 之间发生竞态，Actor 可能没有被重新加入 Scheduler。
```

应先写一个高并发、重复 FlushPending/Signal 的确定性测试，再修改生产代码。

5. 不要通过增加固定等待时间、放宽 Benchmark 超时或删除 QoS 0 失败数据规避问题。

## 当前结论

- Phase 4.6 未完成。
- 12 格开发门禁未通过。
- 严格 MQTTnet 门禁未运行完成。
- 不得启动 Phase 5。
