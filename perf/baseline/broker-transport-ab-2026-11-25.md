# Phase 7-Lite Kestrel / Socket Transport A/B

日期：2026-11-25

## 结论

实验性 `SocketAsyncEventArgs` 明文 TCP 传输未达到保留门槛，已从工作树删除。

核心格 `QoS 0 / 100 订阅者 / 8 KiB` 的三次交替重复中：

| 传输 | 吞吐中位数 | p99 中位数 | 失败数 |
|---|---:|---:|---:|
| Kestrel | 474.17 msg/s | 50,565.1 us | 0 |
| SocketAsyncEventArgs | 421.59 msg/s | 46,625.9 us | 0 |

Socket 的 p99 中位数低约 7.8%，但吞吐中位数低约 11.1%。由于门禁要求吞吐和 p99
同时改善，该原型不得保留，也不能据此宣称专用 Socket 传输优于 Kestrel。

保留下来的架构改动只有内部 `MqttZeroTransportConnection` 边界。Kestrel 仍是唯一
生产传输；实验 Socket、CLI 选择项和 A/B 脚本均已删除。

## 环境

- Windows 11 `10.0.26300`
- Intel Core i7-8565U，4 物理核 / 8 逻辑核
- .NET SDK `10.0.402`
- .NET Runtime `10.0.15`，X64 RyuJIT x86-64-v3
- Server GC、Concurrent GC、TieredPGO：启用
- ThreadPool 最小 Worker：8
- 默认 Event Loop Shards：8
- MQTTnet `5.2.0.1603`

## 原型口径

- 两种传输运行相同的 MQTTZero Broker、协议、Route Epoch、Shard、Egress 和 Session 代码。
- Kestrel 使用现有生产 `ConnectionHandler`。
- 实验传输使用明文 TCP、`SocketAsyncEventArgs`、有界 `System.IO.Pipelines` Pipe、
  Pipe 池化内存和单个 receive/send 在途操作。
- 不包含 TLS、WebSocket、客户端证书或公开 API。
- Broker 子进程独立启动，日志关闭，使用相同 Server GC/ThreadPool 配置。

## 首轮 12 格 smoke

矩阵：QoS `0/1`、订阅者 `1/100`、Payload `32/8192/65536`，预热 `1 s`、
测量 `2 s`、每个传输 `1` 次。该表只用于验证工具链和发现问题，不作为稳定结论。

| QoS | 订阅者 | Payload | Kestrel msg/s | Socket msg/s | Kestrel p99 us | Socket p99 us |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 | 6,809.56 | 6,499.34 | 264.5 | 260.4 |
| 0 | 1 | 8 KiB | 7,003.69 | 6,678.30 | 293.2 | 506.8 |
| 0 | 1 | 64 KiB | 3,227.30 | 2,731.23 | 4,973.8 | 10,248.2 |
| 0 | 100 | 32 | 4,590.43 | 5,223.87 | 860.6 | 1,126.1 |
| 0 | 100 | 8 KiB | 451.30 | 512.01 | 48,271.1 | 36,650.2 |
| 0 | 100 | 64 KiB | 79.32 | 107.28 | 306,966.1 | 74,563.2 |
| 1 | 1 | 32 | 3,314.00 | 3,503.26 | 544.5 | 480.2 |
| 1 | 1 | 8 KiB | 2,841.43 | 3,275.17 | 731.1 | 556.7 |
| 1 | 1 | 64 KiB | 2,246.80 | 3,544.83 | 1,426.4 | 605.1 |
| 1 | 100 | 32 | 222.43 | 232.14 | 7,537.8 | 6,199.4 |
| 1 | 100 | 8 KiB | 171.91 | 163.77 | 10,234.8 | 9,580.6 |
| 1 | 100 | 64 KiB | 48.10 | 47.04 | 48,525.3 | 42,351.6 |

两传输失败数均为 `0`。核心目标格在本次单次运行中曾提升约 13.5% 吞吐并降低
p99，但 64 KiB 单订阅格和 QoS 1/100/8 KiB 同时退化，说明结果不足以稳定保留。

## 核心格交替重复

为了避免先跑完一个传输导致的热状态偏差，三次重复按
`Kestrel -> Socket`、`Socket -> Kestrel`、`Kestrel -> Socket` 交替执行。
每轮预热 `2 s`、测量 `6 s`。

| 传输 | 重复 | 吞吐 msg/s | p99 us | 失败数 |
|---|---:|---:|---:|---:|
| Kestrel | 1 | 554.12 | 22,010.7 | 0 |
| Kestrel | 2 | 474.17 | 50,565.1 | 0 |
| Kestrel | 3 | 471.13 | 57,257.6 | 0 |
| Socket | 1 | 407.45 | 51,511.3 | 0 |
| Socket | 2 | 451.05 | 41,910.5 | 0 |
| Socket | 3 | 421.59 | 46,625.9 | 0 |

## 决策

- 不保留实验 Socket 传输。
- 不继续优化 Socket 原型；其最好样本仍无法证明端到端收益。
- Kestrel 仍然不是已证明的瓶颈，也不能被判定为无罪。当前证据只说明这个
  `SocketAsyncEventArgs` 实现没有产生稳定收益。
- 下一轮应回到 Egress 数据路径，优先验证跨发布有界合批、每连接 Flush 节奏和
  Kestrel PipeWriter 使用方式。
- 原始 JSON 记录位于 `perf/baseline/broker-transport-ab-2026-11-25.json`。

