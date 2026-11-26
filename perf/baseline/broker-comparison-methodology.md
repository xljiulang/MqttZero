# MQTTZero / MQTTnet Broker Comparison

该基准用于比较 MQTTZero 与 MQTTnet 完整 TCP Broker，不再使用“MQTTnet codec + topic matcher”作为端到端基线。

## 公平性口径

- Broker 在独立子进程中启动；客户端负载生成器不与被测 Broker 共享进程。
- 两个 Broker 均绑定 `127.0.0.1`，关闭日志，使用 MQTT 5 客户端。
- 两个 Broker 均通过 Kestrel 承载；MQTTZero 使用 `UseMqttZero`，MQTTnet 使用 `MQTTnet.AspNetCore` 的 `UseMqtt`。
- 客户端 clean start，Topic 固定为 `bench/topic`，不启用 TLS、Retained、共享订阅或会话恢复。
- MQTTZero `MaxMessageQueueLength` 与 MQTTnet `MaxPendingMessagesPerClient` 均设为 100,000。
- 所有 QoS 都使用最多约 1,024 条未消费消息的闭环限制，避免只因负载生成器压垮服务端队列而失去可比性。
- 在途消息窗口可通过 `--max-pending` 调整；不同 Payload 的结果必须同时记录窗口大小。
- 预热结束后必须等待本轮已发布消息全部送达订阅者，再清零计数进入测量；否则 QoS 1/2 的 ACK 与扇出解耦会让预热积压在测量窗口内释放，污染结果。
- 默认预热 15 秒、测量 60 秒；每个矩阵单元独立启动和关闭 Broker。
- 容量门禁默认执行三次，以每个 Broker 独立结果的中位数计算吞吐和 p99 比值。
- 每个单元格的性能声明门禁为吞吐比不低于 MQTTnet 的 `1.00x`，且 p99 不高于 MQTTnet。
- 性能声明门禁与普通 alpha 发布门禁分离。普通 alpha 发布要求正确性、分配哨兵、零失败和相对同机稳定基线吞吐回退不超过 `5%`；未胜出的严格矩阵单元格记录为已知差距。
- `compare` 输出的顶层 `GatePassed` 表示全部严格性能声明单元格是否通过；任一门禁失败时进程返回非零退出码。
- `--comparison-gate false` 只保留结构门禁（缺结果、失败操作、投递完整性与顺序），跳过吞吐/p99 排名；
  该模式只用于 `eng/verify-benchmark-smoke.ps1` 这类"基准链路是否跑通"的冒烟检查，
  性能声明必须使用默认严格模式，且只在专用性能机上判定。

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
  outbound --qos 0 --subscribers 1000 --payload 65536 `
  --warmup 3 --seconds 10
```

`--fragmentation false` 可用于比较 PipeWriter 分段写入和整包写入。`outbound`
模式绕过发布者网络连接，`--subscribers` 用于让多个真实订阅客户端接收消息，
仅用于定位订阅者输出路径。

输出为 JSON，包含预热发布/接收数、发布数、接收数、失败数、每秒吞吐以及延迟
p50/p95/p99。`outbound` 模式还输出 Broker API 等待时间和单次发布耗时。

高扇出、大 Payload 单元格在 6 秒窗口内可能只有十余个发布样本，p99 实际退化为
最大值。正式容量结论应增加测量时长，使每个单元格至少有足够多的发布样本，并
同时记录预热残余数。

## 工具链验证

2026-11-24 使用 `--qos 1 --subscribers 2 --payload 256 --warmup 1 --seconds 2` 对两个 Broker 各执行一次快速比较，均完成连接、订阅、发布和接收，失败数为 0。该短时运行只用于验证进程隔离、负载生成和结果采集，不作为性能结论。

## 结果记录

容量结论必须附带执行日期、机器型号、操作系统、.NET 版本、完整命令和三次运行结果。短时快速验证只用于检查工具链，不作为性能声明。

Session outbound pump 的专项前后对照见 `broker-outbound-pump-2026-11-24.md`。
