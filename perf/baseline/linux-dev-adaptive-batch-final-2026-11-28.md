# Linux 开发服务器自适应大包批次正式确认

## 结论

MQTTZero 对 `>= 32 KiB` PUBLISH 使用内部 `128 KiB / 2 条` 批次上限后：

- 目标格 `QoS1 / 500 / 64 KiB` 三轮吞吐超过 MQTTnet。
- Broker 峰值 RSS 从修复前约 `488 MB` 降到约 `178 MB`，并低于 MQTTnet。
- 保护格吞吐没有出现超过 5% 的回退。
- 目标格 p99 仍略高于 MQTTnet，但差距已从约 8.5% 缩小到约 0.9%。

## 目标格正式三轮

| Broker | 吞吐中位数 | p99 中位数 | Broker CPU / 1000 投递 | 峰值 RSS | 峰值 PSS |
|---|---:|---:|---:|---:|---:|
| MQTTZero | 4222.8 delivery/s | 10479 ms | 66.4 ms | 177.9 MB | 150.2 MB |
| MQTTnet | 4175.0 delivery/s | 10388 ms | 57.6 ms | 187.6 MB | 159.3 MB |
| 比值 | 1.011x | 1.009x | 1.153x | 0.948x | 0.943x |

三轮均完整接收 `50000` 次投递，QoE 覆盖率均达到 99%。

## 保护格

快速单轮：

| 单元格 | MQTTZero delivery/s | MQTTnet delivery/s | 吞吐比 | 结果 |
|---|---:|---:|---:|---|
| QoS1 / 100 / 8 KiB | 10325.5 | 9902.5 | 1.043 | 通过吞吐保护 |
| QoS1 / 1 / 64 KiB | 113.4 | 115.1 | 0.985 | 回退 1.5%，在 5% 保护线内 |

保护格为单轮快速检查，不作为最终性能声明。

## 代码变化

- 新增 `MqttZeroEgressBatchPolicy`。
- 首条消息编码长度达到 `32 KiB` 时，将本轮批次上限收紧为 `128 KiB / 2 条`。
- 64 KiB PUBLISH 编码后略大于 64 KiB，因此实际通常每条消息 Flush 一次。
- 小包和更小调用方配置保持原有行为。

## 诊断证据

修复前 500 订阅者诊断：

```text
累计分配约 3.14 GB
Managed heap 峰值约 743 MB
Gen2 41
RSS 约 488 MB
BufferPool committed 约 13 MB
```

自适应批次三次快速 500 订阅者诊断：

```text
累计分配中位数约 972 MB
RSS 中位数约 185 MB
BufferPool committed 约 13 MB
```

因此主要成本来自每连接输出批次和 Kestrel 输出缓冲，而不是 MQTTZero 的消息 Lease
或 BufferPool。

## 验证

```text
OutboundDelivery / MqttZeroEgress 定向测试：50/50 通过
分配哨兵：24/24 通过
格式检查：通过
目标格正式三轮：吞吐门禁通过
```

## 剩余问题

- 目标格 p99 仍高约 `0.9%`，尚未达到最终“不高于 MQTTnet”的目标。
- CPU 每 1000 次投递仍高约 `15%`。
- 下一步应衡量单条 Flush 后的 Kestrel Pipe 分配，而不是继续扩大批次。
