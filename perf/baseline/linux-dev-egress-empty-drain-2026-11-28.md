# Linux 开发服务器 Egress 空唤醒收尾结果

## 结论

本轮修复关闭了 `QoS1 / 100 / 8 KiB` 的严格 p99 例外：

- MQTTZero p99 中位数：`10 ms`
- MQTTnet p99 中位数：`10 ms`
- p99 比值：`1.000x`
- 投递吞吐比：`0.998x`
- QoE 覆盖率：三次均为 `100%`
- 投递完整性：三次均接收 `50,000 / 50,000`

同时保持：

- `QoS1 / 500 / 64 KiB` 吞吐为 MQTTnet 的 `1.015x`。
- `QoS1 / 1 / 64 KiB` 延迟档 p99 为 `3 / 3 ms`。
- `QoS1 / 100 / 8 KiB` 吞吐快速档为 MQTTnet 的 `1.100x`。

本结果只适用于当前 `linux-dev-2vcpu` 开发环境，不构成容量或全矩阵领先声明。

## 环境

| 项目 | 值 |
|---|---|
| 主机 | `linux-dev-2vcpu` |
| CPU | AMD EPYC 7K62，2 vCPU |
| 内存 | 3904782336 B |
| 操作系统 | Ubuntu 24.04 |
| .NET SDK | 10.0.115 |
| eMQTT-Bench | 0.6.4 |
| Broker 对照 | MQTTnet Server |

性能证据提交：

- Task 1：`3c292b9`
- Task 2：`e8d7b4d`
- Task 3：`a388dff`
- 最终等价提交：`dbbabcd`
- 最终吞吐证据提交：`1724154`

Task 3 的最终提交 `dbbabcd` 与证据提交 `a388dff` 只差一个源码声明由两行改为单行，
运行时语义相同。

## 诊断阶段

命令：

```powershell
$env:MQTTZERO_SSH_CONFIG='D:/ssh/tencent/linux-dev.json'
$env:ENABLE_FANOUT_DIAGNOSTICS='1'
$env:ENABLE_MQTTZERO_MEMORY_PROBE='1'
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency -Qos 1 -Subscribers 100 -Payload 8192 `
  -Repetitions 1 -StartPort 20100 `
  -Campaign egress-empty-drain-baseline
```

关键计数：

| 指标 | 值 |
|---|---:|
| EgressDataBatchCount | 55,000 |
| EgressDrainCount | 110,697 |
| Drain / Batch | 2.013x |
| EgressEmptyDataDrainCount | 54,943 |
| EgressSignalWithoutPendingCount | 54,948 |
| EgressSynchronousFlushCount | 55,000 |
| EgressReadyToExecuteCount | 110,697 |
| TotalEgressReadyToExecute | 17,146.50 ms |
| MaxEgressReadyToExecute | 39.47 ms |
| FlushWaitCount | 0 |

结论：Drain 次数约为数据批次的两倍；其中 54,943 次没有待发数据，而且所有 Flush
均同步完成。主要浪费来自 ACK 后空 Signal 和 Egress Scheduler 中转，不是异步 Flush。

## Task 2：跳过空 Signal

命令：

```powershell
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency -Qos 1 -Subscribers 100 -Payload 8192 `
  -Repetitions 1 -StartPort 20200 `
  -Campaign egress-empty-drain-removed
```

关键计数：

| 指标 | 修复后 |
|---|---:|
| EgressDataBatchCount | 55,000 |
| EgressDrainCount | 55,754 |
| Drain / Batch | 1.013x |
| EgressEmptyDataDrainCount | 0 |
| EgressSignalWithoutPendingCount | 54,905 |
| EgressSynchronousFlushCount | 55,000 |
| EgressReadyToExecuteCount | 55,754 |
| TotalEgressReadyToExecute | 13,893.37 ms |
| MaxEgressReadyToExecute | 36.10 ms |

单轮 CPU 从 `2230 ms` 降到 `2130 ms`，投递吞吐保持 `4633.9 delivery/s`。

正式延迟档仍为 MQTTZero `10 ms` 对 MQTTnet `9 ms`，因此继续执行 Task 3。

## Task 3：同 Shard 本地 Egress 续排

诊断单轮：

| 指标 | 值 |
|---|---:|
| EgressDataBatchCount | 55,000 |
| EgressDrainCount | 55,754 |
| Drain / Batch | 1.010x |
| EgressEmptyDataDrainCount | 0 |
| EgressReadyToExecuteCount | 55,754 |
| TotalEgressReadyToExecute | 15,648.04 ms |
| MaxEgressReadyToExecute | 39.95 ms |
| EgressSchedulerPassCount | 1,571 |

单轮最大交付延迟从 Task 2 的 `28 ms` 降到 `15 ms`。

### `QoS1 / 100 / 8 KiB` 正式延迟档

命令：

```powershell
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode latency -Qos 1 -Subscribers 100 -Payload 8192 `
  -Repetitions 3 -StartPort 20500 `
  -Campaign egress-local-follow-up-latency-v2
```

三轮原始结果：

| Run | Broker | delivery/s | p50 | p95 | p99 | max | QoE |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | MQTTZero | 4618.30 | 5 ms | 8 ms | 10 ms | 17 ms | 100% |
| 1 | MQTTnet | 4626.78 | 5 ms | 8 ms | 10 ms | 16 ms | 100% |
| 2 | MQTTZero | 4616.44 | 4 ms | 8 ms | 10 ms | 15 ms | 100% |
| 2 | MQTTnet | 4636.79 | 5 ms | 8 ms | 10 ms | 18 ms | 99% |
| 3 | MQTTZero | 4616.81 | 5 ms | 8 ms | 10 ms | 15 ms | 100% |
| 3 | MQTTnet | 4625.93 | 5 ms | 8 ms | 9 ms | 13 ms | 100% |

中位数：

| Broker | delivery/s | p50 | p95 | p99 | CPU / 1000 投递 |
|---|---:|---:|---:|---:|---:|
| MQTTZero | 4616.8 | 5 ms | 8 ms | 10 ms | 44.0 ms |
| MQTTnet | 4626.8 | 5 ms | 8 ms | 10 ms | 41.6 ms |
| 比值 | 0.998x | 1.000x | 1.000x | 1.000x | 1.058x |

结果：严格 p99 门禁通过。

### `QoS1 / 500 / 64 KiB` 正式吞吐

命令：

```powershell
pwsh -NoProfile -File eng/emqtt-bench/run-linux-dev.ps1 `
  -Mode throughput -Qos 1 -Subscribers 500 -Payload 65536 `
  -Repetitions 3 -StartPort 20600 `
  -Campaign egress-local-follow-up-target
```

| Broker | delivery/s | p95 | p99 | CPU / 1000 投递 | Peak RSS |
|---|---:|---:|---:|---:|---:|
| MQTTZero | 4254.9 | 10002 ms | 10399 ms | 60.4 ms | 173.1 MB |
| MQTTnet | 4191.3 | 9997 ms | 10377 ms | 56.2 ms | 194.7 MB |
| 比值 | 1.015x | 1.001x | 1.002x | 1.075x | 0.889x |

结果：吞吐通过，内存低于 MQTTnet；p99 回退 `0.2%`，在 `5%` 保护线内。

### 保护格

`QoS1 / 1 / 64 KiB` 延迟档：

| Broker | delivery/s | p95 | p99 |
|---|---:|---:|---:|
| MQTTZero | 46.2 | 2 ms | 3 ms |
| MQTTnet | 46.2 | 2 ms | 3 ms |
| 比值 | 1.000x | 1.000x | 1.000x |

`QoS1 / 1 / 64 KiB` 吞吐快速档：

| Broker | delivery/s |
|---|---:|
| MQTTZero | 169.2 |
| MQTTnet | 114.7 |
| 比值 | 1.475x |

`QoS1 / 100 / 8 KiB` 吞吐正式档：

| Broker | delivery/s |
|---|---:|
| MQTTZero | 11674.7 |
| MQTTnet | 10463.8 |
| 比值 | 1.116x |

该吞吐档三次均完整投递 `100,000` 次；p95/p99 是积压排空诊断，不作为延迟门禁。

## 验证

- Release 全解决构建：通过，0 warning，0 error。
- 核心测试：`838/838`。
- Broker Benchmark 测试：`71/71`。
- 分配哨兵：`24/24`。
- `dotnet format --verify-no-changes --no-restore`：通过。
- NuGet 四包构建与仓库外 Shared/Dashboard 消费者：通过。
- 漏洞扫描：无发现。
- 弃用包扫描：无发现。
- Dashboard 和 Vue 测试按既有要求未运行。

## 原始结果

- `artifacts/emqtt-bench/egress-empty-drain-baseline`
- `artifacts/emqtt-bench/egress-empty-drain-removed`
- `artifacts/emqtt-bench/egress-empty-drain-latency`
- `artifacts/emqtt-bench/egress-local-follow-up-latency-v1`
- `artifacts/emqtt-bench/egress-local-follow-up-latency-v2`
- `artifacts/emqtt-bench/egress-local-follow-up-target`
- `artifacts/emqtt-bench/egress-local-follow-up-guard-latency-v2`
- `artifacts/emqtt-bench/egress-local-follow-up-guard-throughput`
- `artifacts/emqtt-bench/egress-local-follow-up-throughput`
- `artifacts/emqtt-bench/egress-local-follow-up-throughput-formal`

## 局限

- 当前只有 2 vCPU 开发服务器，结果仍是开发级证据。
- QoE 延迟分辨率为 1 ms；本轮 p99 闭合表示三次中位数不高于 MQTTnet，不代表所有
  payload、订阅者数和 QoS 的组合都已严格领先。
- `EgressSignalCount` 未进入现有 `MEMORYPROBE` schema；空 Signal 的抑制由
  `EgressSignalWithoutPendingCount`、空 Drain 降为 0 和 Drain/Batch 从 `2.013x`
  降到 `1.013x` 共同证明。
