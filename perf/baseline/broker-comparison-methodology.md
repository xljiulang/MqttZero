# MQTTZero / MQTTnet Broker Comparison

该基准用于比较 MQTTZero 与 MQTTnet 完整 TCP Broker，不再使用“MQTTnet codec + topic matcher”作为端到端基线。

## 公平性口径

- Broker 在独立子进程中启动；客户端负载生成器不与被测 Broker 共享进程。
- 两个 Broker 均绑定 `127.0.0.1`，关闭日志，使用 MQTT 5 客户端。
- 客户端 clean start，Topic 固定为 `bench/topic`，不启用 TLS、Retained、共享订阅或会话恢复。
- MQTTZero `MaxMessageQueueLength` 与 MQTTnet `MaxPendingMessagesPerClient` 均设为 100,000。
- QoS 0 使用最多约 1,024 条未消费消息的闭环限制，避免仅因负载生成器压垮服务端队列而失去可比性。
- QoS 0 的在途消息窗口可通过 `--max-pending` 调整；不同 Payload 的结果必须同时记录窗口大小。
- 默认预热 15 秒、测量 60 秒；每个矩阵单元独立启动和关闭 Broker。
- 容量门禁默认执行三次，以每个 Broker 独立结果的中位数计算吞吐和 p99 比值。

## 默认矩阵

- QoS：0、1、2
- 订阅者：1、100、1000
- Payload：32 B、8 KiB、64 KiB
- 发布者：1

## 运行

完整矩阵：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare
```

快速验证单个场景：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 1 --subscribers 10 --payload 1024 --warmup 2 --seconds 10
```

三次中位数、固定 8 MiB 在途窗口：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 0,1 --subscribers 1 --payload 8192 --repetitions 3 `
  --max-pending 1024 --warmup 3 --seconds 10
```

Broker API 出站诊断：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  outbound --qos 0 --payload 8192 --warmup 3 --seconds 10
```

`--fragmentation false` 可用于比较 PipeWriter 分段写入和整包写入。`outbound`
模式绕过发布者网络连接，仅用于定位订阅者输出路径。

输出为 JSON，包含发布数、接收数、失败数、每秒吞吐以及延迟 p50/p95/p99。

## 工具链验证

2026-11-24 使用 `--qos 1 --subscribers 2 --payload 256 --warmup 1 --seconds 2` 对两个 Broker 各执行一次快速比较，均完成连接、订阅、发布和接收，失败数为 0。该短时运行只用于验证进程隔离、负载生成和结果采集，不作为性能结论。

## 结果记录

容量结论必须附带执行日期、机器型号、操作系统、.NET 版本、完整命令和三次运行结果。短时快速验证只用于检查工具链，不作为性能声明。

Session outbound pump 的专项前后对照见 `broker-outbound-pump-2026-11-24.md`。
