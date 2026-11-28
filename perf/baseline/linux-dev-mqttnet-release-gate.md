# Linux 开发服务器 MQTTnet 严格门禁

## 状态

**阻塞。** 主目标吞吐和 p99 已通过，但保护格 `QoS1 / 100 / 8 KiB` 的
正式延迟档 p99 中位数为 MQTTnet 的 `1.125x`，超过严格门禁。

因此：

- PR1 性能工具链可以进入合并流程。
- PR2 Egress 诊断和大包批次必须保留为未合并分支。

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

正式延迟档补足五轮：

| Broker | p95 中位数 | p99 中位数 |
|---|---:|---:|
| MQTTZero | 7 ms | 9 ms |
| MQTTnet | 7 ms | 8 ms |
| 比值 | 1.000x | 1.125x |

结果：**失败**。p95 持平，p99 多 1 ms。

诊断：

```text
平均 Egress Batch：1 条
FlushWait：0
PendingDepth：1
InflightDepth：2
吞吐：与 MQTTnet 持平
CPU/1000 投递：更高
```

全局 `preferLocal=true` 可偶尔改善该单元格，但会让主目标
`QoS1 / 500 / 64 KiB` 吞吐降到 `0.928x`，已回滚。

原始结果：

- `strict-guard-latency-q1-100-8k`
- `strict-guard-latency-q1-100-8k-extra2`
- `scheduling-latency-q1-100-8k`

## 混合大小包修复

已修复：

- 小包后的第一个大包必须从新批次开始。
- 大包可以与同为大包开头的批次合并。
- 小包、大包和交替消息保持队列顺序与完整包边界。

定向测试通过。

## 未完成的门禁

- `QoS1 / 100 / 8 KiB` p99 需要降至 `<=8 ms`，或证明该单元格属于工具/
  测量分辨率不可判定的噪声并取得明确策略批准。
- PR2 不能合并。
- 版本不提升，不修改发布声明。
