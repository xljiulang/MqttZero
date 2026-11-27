# Broker A/B、交付完整性与内存探针

> 上级索引：[文档目录](../README.md)

本文说明 `0.11.0-alpha.3` 之后新增的测量工具。它不改动 Broker 的调度模型，
只回答三个问题：候选提交是否更快、交付是否完整、内存是否回到基线。

## 同机 A/B

`eng/compare-broker-ab.ps1` 把基线检出和候选检出各构建一次，然后用**同一个
负载驱动**交替运行。奇数轮先跑基线，偶数轮先跑候选，以抵消机器漂移。

每个单元格单独启动一个进程，因此可以按小批次执行并在中断后续跑：

```powershell
# 单个单元格，3 轮
pwsh -NoProfile -File ./eng/compare-broker-ab.ps1 `
    -BaselineRef v0.11.0-alpha.3 `
    -Qos 1 -Subscribers 100 -PayloadSizes 65536 `
    -Rounds 3 -WarmupSeconds 5 -MeasurementSeconds 20 `
    -OutputDirectory artifacts/broker-ab/target

# 其余保护格作为独立批次
pwsh -NoProfile -File ./eng/compare-broker-ab.ps1 `
    -BaselineRef v0.11.0-alpha.3 `
    -Qos 0,1,2 -Subscribers 1,500 -PayloadSizes 32,8192 `
    -Rounds 3 -SkipExisting
```

`-BaselinePath` 指向已有的基线检出；省略时脚本在 `.worktrees/` 下创建
`-BaselineRef` 的工作树。`-SkipExisting` 跳过已有 JSON 的单元格。

每次运行记录：提交、OS、.NET、GC 模式、实际 Shard 数与 Kestrel I/O 队列数、
Broker 与负载进程各自的 CPU / 工作集 / 分配量、端到端吞吐、发布调用 p99、
订阅端交付 p99、排空时间与失败数。

### 门禁

- 主目标格 `QoS 1 / 100 订阅者 / 64 KiB`：候选端到端吞吐中位数提升 ≥ 10%，
  交付 p99 恶化 ≤ 5%。
- 保护格：吞吐不得下降超过 5%，交付 p99 不得恶化超过 5%。
- 所有格：零失败操作、零缺失交付、零重复交付、零序号回退。
- 不满足时保持现有默认配置；只有特定拓扑受益时只发布调优建议。

与 MQTTnet 的对比仍由 `eng/run-broker-benchmark.ps1` 单独报告，不作为
本轮发布门禁。

### 消息过期与订阅端节流

`--message-expiry <秒>` 让发布携带 Message Expiry Interval，
`--subscriber-delay-us <微秒>` 在记录交付延迟后延迟返回。raw 客户端与
`--message-expiry` 组合会被拒绝。

```powershell
dotnet <benchmarks.dll> ab `
    --baseline-broker <dll> --candidate-broker <dll> `
    --qos 0 --subscribers 1 --payload 32 --message-expiry 300 `
    --subscriber-delay-us 200 --max-pending 100000 `
    --warmup 5 --seconds 20 --rounds 3
```

注意：MQTTnet 客户端在接收处理被延迟时仍会继续读取，因此这个 profile
实际不会把 Broker 侧队列压深（实测队列深度只有个位数），不能用来证明
「出站批量选择」的复杂度变化。要复现长队列需要真正停滞的消费者，例如
暂停读取的 raw 客户端；该能力尚未加入基准。

## 交付完整性

每个 PUBLISH 负载的前 16 字节是投递信封：8 字节序号 + 8 字节发布时间戳
（同一进程的单调时钟）。订阅端据此：

- 记录订阅端交付延迟，与发布调用 p99 分开统计；
- 统计序号回退，从而识别重复或乱序；
- 用最大序号与发布总数比较，识别缺口。

因此「一条丢失 + 一条重复」不会被总数抵消。多发布者运行时全局序号不保证
有序，只校验缺口与信封，不校验顺序。

## 内存探针

`serve` 模式支持：

```powershell
dotnet <benchmarks.dll> serve --broker mqttzero --port 19410 `
    --max-buffered-bytes 8388608 --memory-probe-ms 100
```

探针每 100 ms 在 stderr 输出一行 `MEMORYPROBE {...}`，包含缓冲池租出/缓存/
提交字节、路由计划在用数、共享游标数、各对象池数量、托管堆、工作集与分配量。
收到 stdin 的 `PROBE-GC` 时先做一次紧凑回收再采样，用于排空后的干净堆读数。

`memory` 模式加 `--broker-assembly <dll>` 时，Broker 在独立进程运行，
负载进程通过 TCP 发布并订阅。每波记录：提交字节峰值、路由计划在用数、
排空后的托管堆与空闲基线租出字节。

```powershell
# 日常 10 波
pwsh -NoProfile -File ./eng/verify-memory-stability.ps1 -Waves 10

# 发布前 30 波，并加入 8 MiB 配额压力场景
pwsh -NoProfile -File ./eng/verify-memory-stability.ps1 `
    -Waves 30 -Pressure
```

门禁：提交字节（含峰值）不超过配置上限；排空后路由计划在用数为零；
租出字节回到同场景空闲基线（允许 64 KiB 偏差）；托管堆以第 6–10 波与
第 26–30 波（不足时取首尾 5 波）的中位数比较，增长不超过 5% + 1 MiB。
工作集只记录峰值与趋势。

## 稳态场景

`tests/MqttZero.BrokerBenchmarks.Tests/BrokerSteadyStateTests.cs` 覆盖：

- 慢订阅者暂停读取后恢复，期间 Broker 占用不超过缓冲上限；
- 连续订阅/退订后路由计划与共享游标回到基线；
- 停止时仍有待发送数据，Broker 在限时内完成并归还全部租出缓冲区。

这些测试断言资源归属，不产生吞吐结论。

## 测量口径

跨 MQTTZero/MQTTnet 的最终性能结果改由 `eng/emqtt-bench/run-matrix.sh`
生成。`load`、`compare` 和 `ab` 仍使用完整保留的 Raw 诊断客户端，用于
微秒级直方图、顺序/重复/缺失校验和暂停读取背压；这些内部结果不得与
eMQTT-Bench 的第三方客户端结果混为同一统计口径。

A/B 单元格先算 `DrainRatio = 排空时间 / (测量时间 + 排空时间)`（每轮写入
`BrokerBenchmarkResult.DrainRatio`）。四轮中位数超过 `0.10` 时，该单元格改用
`PublishWindowThroughputPerSecond` 判定并在报告里记录 `ThroughputMetric = publish-window`，
否则用端到端吞吐。`qos1-subs1-payload65536` 就是典型例子：6 s 窗口配 3–6 s 排空，
端到端数字一半以上是排空尾巴。

两个变体的 `--event-loop-shards` 请求值必须一致，否则报告直接判失败，避免跨拓扑比较。

延迟敏感的单元格使用「延迟档」：把客户端的 `--max-pending` 收到 64，让队列深度受控，
再用发布/投递 p99 判定；吞吐档仍用 1024。

单订阅 64 KiB 要再压一档：`--max-pending 64` 下 `DrainRatio` 仍有 0.39，吞吐只能按 publish-window 判；
收到 `--max-pending 8` 后 `DrainRatio` 降到 0.001、端到端口径重新可判（5 轮吞吐 +1.5%、交付 p99 比 0.961）。
两个档位的用途不同：64 用于 20 s 窗口下的 publish-window 口径，8 用于端到端延迟口径。

```powershell
# 吞吐档：主目标与保护格
pwsh -NoProfile -File ./eng/compare-broker-ab.ps1 `
    -BaselineRef master -Qos 1,0 -Subscribers 100 -PayloadSizes 65536,32768 `
    -Rounds 3 -WarmupSeconds 5 -MeasurementSeconds 20

# 延迟档：单订阅大包与 QoS 0 单订阅
pwsh -NoProfile -File ./eng/compare-broker-ab.ps1 `
    -BaselineRef master -Qos 1,0 -Subscribers 1 -PayloadSizes 65536,32 `
    -MaxPending 64 -Rounds 3 -WarmupSeconds 5 -MeasurementSeconds 20
```

QoS 0 单订阅的 p99 落在 500 µs 批窗口噪声里，比值不稳定，用
`-DeliveryP99AbsoluteUs 2000` 改成绝对预算判定，比值只作记录。
