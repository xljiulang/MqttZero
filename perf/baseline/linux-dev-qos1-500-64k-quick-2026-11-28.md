# Linux 开发服务器 QoS1 / 500 / 64 KiB 快速基线

## 结论

这是一次单轮快速检查，不构成发布级性能声明。目标格当前仍未达到 MQTTnet：

| 指标 | MQTTZero | MQTTnet | 比值 |
|---|---:|---:|---:|
| 端到端投递吞吐 | 4288.2 delivery/s | 4611.5 delivery/s | 0.930x |
| p50 交付完成时间 | 5566 ms | 5190 ms | 1.072x |
| p95 交付完成时间 | 10188 ms | 9700 ms | 1.050x |
| p99 交付完成时间 | 10549 ms | 9976 ms | 1.057x |
| Broker CPU / 1000 次投递 | 73.2 ms | 64.4 ms | 1.137x |
| Broker 峰值 RSS | 494.5 MB | 190.5 MB | 2.599x |
| Broker 峰值 PSS | 480.6 MB | 181.8 MB | 2.644x |
| 客户端 CPU | 15.46 s | 14.83 s | 1.043x |

两个 Broker 均完整接收 `100 × 500 = 50000` 次投递，失败数为 0。

## 环境

| 项目 | 值 |
|---|---|
| 日期 | 2026-11-28 |
| 主机 | `linux-dev-2vcpu` |
| CPU | AMD EPYC 7K62，2 vCPU |
| 内存 | 3813264 KiB |
| 操作系统 | Ubuntu 24.04 |
| .NET SDK | 10.0.115 |
| eMQTT-Bench | 0.6.4 |
| 文件描述符上限 | 1048576 |
| 提交 | `9feb7df4332681c3dc94474925aae6db9cb6d14b` |

测试前已停止 `intercom` Docker Compose 项目。测试期间没有其他 MQTTZero、
MQTTnet 或 eMQTT-Bench 负载进程。

## 命令

```powershell
# 执行环境中已设置 MQTTZERO_SSH_CONFIG
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Quick `
  -Mode throughput `
  -Qos 1 `
  -Subscribers 500 `
  -Payload 65536 `
  -StartPort 19400 `
  -Campaign quick-q1-500-64k-v1
```

`-Quick` 使用单轮和 1 秒 QoE Flush 等待；本次两个 Broker 的 QoE 覆盖率均达到
100%，但仍不得把单轮 p99 用作发布门禁。

## 解读

- 吞吐差距为 7.0%，不是数量级问题。
- CPU 每次投递差距约为 13.7%，可以解释部分吞吐差距。
- RSS/PSS 差距约为 2.6 倍，且业务容器已经停止，因此内存问题属于 MQTTZero
  自身路径，而不是同机其他服务污染。
- 下一个诊断必须拆分 BufferPool rented/cached/committed、Egress 待 Flush 字节、
  队列深度和对象池保留量。

## 有效性边界

- 本轮只有一个 repetition，不能用于最终中位数门禁。
- 本轮用于快速趋势和资源归因，不用于容量声明。
- 正式结论仍需同机三轮 MQTTZero/MQTTnet 交替测试。
