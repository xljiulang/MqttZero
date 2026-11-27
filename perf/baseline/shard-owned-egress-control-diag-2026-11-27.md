# Shard-Owned Egress 控制帧诊断与 QoS 2 尾延迟修复（2026-11-27）

## 结论

- QoS 2 的控制帧 **100% 走分片内直写**（`ControlFrameQueuedCount = 0`），每帧一个批次、每批一帧：
  `ControlBatchCount / DeliveryCount ≈ 1.02`，平均批大小 1.0。QoS 1 同一指标只有 0.013。
- 同一批投放里，QoS 2 的控制写累计耗时约 385–489 ms，单次最长 5.8–9.3 ms；QoS 1 约 6–7 ms，
  单次最长 1.7–2.4 ms。控制写本身不是吞吐主因，但 9 ms 级别的单次阻塞发生在分片线程上。
- QoS 2 每个投递对应约 2.0 个 ACK 处理（`AckWorkCount / DeliveryCount = 2.01`），QoS 1 为 1.0，
  与报文语义一致，仪表自检通过。
- 把带握手的出站确认（PUBREL、PUBCOMP）改为经 egress 队列写出后，QoS 2 / 100 / 8 KiB 在
  5 轮 × 20 s 窗口下从「p99 劣化 20–41%」变为「吞吐 −4.0%、p99 比 1.007」，通过门禁。
- PUBACK、PUBREC **不能**改为入队：入队的 PUBREC 会被紧随其后的 DISCONNECT 越过，
  `MaxAwaitingRelIsIndependentFromOutboundInflight` 直接捕获（收到 DISCONNECT 而非 PUBREC）。
  最终实现只对 PUBREL、PUBCOMP 入队。

## 方法与环境

| 项 | 值 |
|---|---|
| 基线 | `master@ec8ba3f`（worktree `.worktrees/ab-master`） |
| 候选 | `refactor/shard-owned-egress-timers` + 本轮提交 |
| 机器 | 12th Gen Intel Core i5-12450H，Windows `10.0.26300`，.NET `10.0.15` |
| 拓扑 | `EventLoopShardCount=0` → 8，Kestrel I/O 队列 8 |
| 客户端 | MQTTnet，单发布者，`--max-pending 1024` |
| 诊断通道 | `ab --diagnostics true`；serve 每 1 s 向 stderr 写 `CONTROL {json}`，父进程持续排空并取每轮最后一条 |

诊断轮开启计数器（`Interlocked` 开销、`--diagnostics` 关闭时零成本），其吞吐数字不作门禁依据。
基线二进制没有 `CONTROL` 探针（master 缺少计数器），因此控制帧指标只在候选侧给出；数据批指标
两变体都有，但无法从基线取到探针，故以候选内 QoS 2 / QoS 1 对照。

## 诊断结果（候选，3 轮，2 s 预热 / 6 s 测量）

### QoS 2 / 100 / 8 KiB

| 轮 | 投递 | 控制帧 | 直写 | 入队 | 控制批 | 批内帧合计 | 批最大 | 控制写合计 | 控制写最大 | ACK 处理 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 53,400 | 54,465 | 54,465 | 0 | 54,465 | 54,465 | 1 | 489.0 ms | 9.29 ms | 107,084 |
| 2 | 61,000 | 62,301 | 62,301 | 0 | 62,301 | 62,301 | 1 | 384.6 ms | 7.62 ms | 122,501 |
| 3 | 70,400 | 71,907 | 71,907 | 0 | 71,907 | 71,907 | 1 | 393.1 ms | 5.82 ms | 141,423 |

### QoS 1 / 100 / 64 KiB

| 轮 | 投递 | 控制帧 | 直写 | 入队 | 控制批 | 控制写合计 | 控制写最大 | ACK 处理 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 26,400 | 364 | 100 | 264 | 364 | 6.40 ms | 1.66 ms | 26,345 |
| 2 | 31,600 | 416 | 100 | 316 | 416 | 7.07 ms | 2.35 ms | 31,600 |
| 3 | 32,000 | 420 | 100 | 320 | 420 | 7.39 ms | 2.33 ms | 31,910 |

### 指标判读

| 指标 | 公式 | QoS 2 | QoS 1 | 判读 |
|---|---:|---:|---:|---|
| R1 控制批/投递 | `ControlBatchCount / DeliveryCount` | 1.02 | 0.013 | 候选 QoS 2 每投递一次控制写 |
| R2 平均控制批 | `ControlBatchFrameSum / ControlBatchCount` | 1.0 | 1.0 | 每批只有一帧，无合并 |
| R3 ACK/投递 | `AckWorkCount / DeliveryCount` | 2.01 | 1.00 | 与 QoS 语义一致，仪表正确 |
| D1 数据批消息数 | `EgressBatchMessageCount / EgressDataBatchCount` | 1.001 | 1.000 | 两格都是「每批一条」，无切碎差异 |
| Q1 ACK 排队 | `MaxAckWorkQueueTicks` | 0 | 0 | 稳态 ACK 走同步分片调用，未经过 work 队列 |

Q1 为 0 说明 `EnqueuedAcknowledgementAsync` 的快路径没有被使用，ACK 排队时延当前不可测；
需要时应在同步分片调用路径上补时间戳，不能据此认为没有排队。

## 修复与 A/B

修复内容：`SendPubRelAsync`、`SendPubRelWithoutWaitingAsync`、`SendPubCompAsync` 在分片内改为
入队（`coalesceOnShard: true`），由 egress 批次写出；CONNACK/SUBACK/UNSUBACK/DISCONNECT/PINGRESP
与 PUBACK/PUBREC 保持直写。

| 组 | 变更 | 轮数 | 窗口 | 基线 msg/s | 候选 msg/s | 吞吐 | p99 比 | 判定 |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `w3-qos2` | PUBACK/PUBREC/PUBREL/PUBCOMP 全部入队 | 3 | 6 s | 70.58 | 85.85 | +21.6% | 0.690 | 通过 |
| `w3-qos2b` | 同上 | 5 | 6 s | 86.62 | 84.72 | −2.2% | 1.060 | 不通过（p99 边界） |
| `w3-qos2-long` | 同上 | 3 | 20 s | 96.30 | 80.74 | −16.2% | 1.298 | 不通过（含一轮离群） |
| `w3-qos2-long5` | 同上 | 5 | 20 s | 96.77 | 94.88 | −2.0% | 0.983 | 通过 |
| `w3-qos1` | 含 PUBACK 入队 | 3 | 6 s | 50.23 | 49.81 | −0.8% | 1.038 | 通过 |
| `w3b-qos2-long5` | 仅 PUBREL/PUBCOMP 入队 | 5 | 20 s | 106.68 | 102.30 | −4.0% | 1.007 | 通过 |

`w3-qos2-long` 的失败来自单轮：该组候选三轮为 97.76 / 67.89 / 80.74，基线为 96.30 / 65.86 / 97.61，
两台变体在同一轮同步掉速（67.9 vs 65.9 时候选更快），只有第三轮候选单独走低。改用 5 轮后恢复。

## 测试发现

PUBREC 入队后，`AcceptInboundQos2Async` 的 Receive Maximum 分支先入队 PUBREC、随后直接写 DISCONNECT，
DISCONNECT 先到达，客户端拿不到 PUBREC。`MaxAwaitingRelIsIndependentFromOutboundInflight` 因此失败。
结论固定为：**凡是后面可能紧跟关闭连接的确认都不能入队**。该用例保留为回归。

## 原始数据

- `artifacts/broker-ab/diag-qos2/`、`artifacts/broker-ab/diag-qos1/`：诊断轮（含 `ControlProbe`）。
- `artifacts/broker-ab/w3-qos2/`、`w3-qos2b/`、`w3-qos2-long/`、`w3-qos2-long5/`、`w3-qos1/`、`w3b-qos2-long5/`：A/B 组。

## 后续

- QoS 2 尾延迟已回到门禁内，但 20 s 窗口下吞吐仍有 −4%；合并前需在同一窗口复测 QoS 1 / 100 / 64 KiB 与
  QoS 0 / 100 / 32 B 两个保护格。
- `AckWorkQueueTicks` 目前恒为 0，若继续追查 ACK 排队，应给同步分片调用补时间戳，而不是相信该字段。
- 测量口径（`DrainRatio`、延迟档 `max-pending`、QoS 0 绝对 p99 预算）仍未实施，属独立分支。
