# Linux 开发服务器 MQTTnet 严格门禁

## 状态

**严格门禁通过。** 主目标吞吐和 p99 已通过。后续 Egress 空唤醒修复将
`QoS1 / 100 / 8 KiB` 的三次正式 p99 中位数从 `9 ms` 对 `8 ms` 收敛到
`10 ms` 对 `10 ms`，p99 比值 `1.000x`。

该单元格不再需要显式例外。结果仍只适用于当前 2 vCPU Linux 开发服务器，
不构成全矩阵领先声明。

## 环境

| 项目 | 值 |
|---|---|
| 主机 | `linux-dev-2vcpu` |
| CPU | AMD EPYC 7K62，2 vCPU |
| 内存 | 3813264 KiB |
| 操作系统 | Ubuntu 24.04 |
| .NET SDK | 10.0.115 |
| eMQTT-Bench | 0.6.4 |
| 客户端 | MQTTnet Server 对照，eMQTT-Bench 施压 |

测试期间停止 `intercom` Docker Compose，关闭 MSBuild/Roslyn 构建服务器。

## 主目标：QoS1 / 500 / 64 KiB

三轮后 MQTTZero 吞吐离散超过 10%，按规则追加到五轮。

五轮：

| Broker | 吞吐中位数 | p95 中位数 | p99 中位数 | 峰值 RSS 中位数 |
|---|---:|---:|---:|---:|
| MQTTZero | 4598.9 delivery/s | 9798 ms | 10168 ms | 178.5 MB |
| MQTTnet | 4250.4 delivery/s | 9944 ms | 10256 ms | 188.1 MB |
| 比值 | 1.082x | 0.985x | 0.991x | 0.949x |

结果：吞吐、p95、p99 和内存均通过。

原始结果：

- `strict-current-q1-500-64k-v1`
- `strict-current-q1-500-64k-extra2`

## 保护格

### QoS1 / 1 / 64 KiB

吞吐档正式三轮：

| Broker | 吞吐 |
|---|---:|
| MQTTZero | 116.4 delivery/s |
| MQTTnet | 118.4 delivery/s |
| 比值 | 0.983x |

结果：吞吐回退 1.7%，在 5% 保护线内。

正式延迟档三轮：

| Broker | p95 | p99 |
|---|---:|---:|
| MQTTZero | 2 ms | 3 ms |
| MQTTnet | 2 ms | 3 ms |

结果：通过。

### QoS1 / 100 / 8 KiB

Egress 空唤醒修复后的正式延迟档三次：

| Broker | p95 中位数 | p99 中位数 |
|---|---:|---:|
| MQTTZero | 8 ms | 10 ms |
| MQTTnet | 8 ms | 10 ms |
| 比值 | 1.000x | 1.000x |

结果：**通过**。三次投递均完整，QoE 覆盖率为 `99%–100%`，交付吞吐比为
`0.998x`。

诊断：

```text
平均 Egress Batch：1 条
FlushWait：0
PendingDepth：1
InflightDepth：2
Drain / Batch：2.013x -> 1.013x
Empty Drain：54943 -> 0
```

做法是：

- ACK 后待发队列为空时不再 Signal Egress。
- 同 Shard Fanout Scheduled Batch 完成后直接执行已就绪 Egress Actor。

原始结果：

- `egress-empty-drain-baseline`
- `egress-empty-drain-removed`
- `egress-empty-drain-latency`
- `egress-local-follow-up-latency-v1`
- `egress-local-follow-up-latency-v2`

## 混合大小包修复

已修复：

- 小包后的第一个大包必须从新批次开始。
- 大包可以与同为大包开头的批次合并。
- 小包、大包和交替消息保持队列顺序与完整包边界。

定向测试通过。

## 未完成的门禁

- `QoS1 / 100 / 8 KiB` p99 的三次中位数已与 MQTTnet 持平。
- 后续优化不得让该单元格回退。
- 仍不把该结果描述为严格全矩阵 p99 通过，因为完整 27 格矩阵尚未重跑。
- 版本不提升，不修改发布声明。

后续修复和原始结果见
`perf/baseline/linux-dev-egress-empty-drain-2026-11-28.md`。
