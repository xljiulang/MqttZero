# Phase 4 Route Epoch and Sharded Fanout Baseline

日期：2026-11-25

## 环境

- Windows 11 `10.0.26300`
- Intel Core i7-8565U，4 物理核 / 8 逻辑核
- .NET SDK `10.0.402`
- .NET Runtime `10.0.15`，X64 RyuJIT x86-64-v3
- Server GC：启用
- Concurrent GC：启用
- TieredPGO：启用
- ThreadPool 最小 Worker：8
- MQTTnet `5.2.0.1603`

## 正式命令

```powershell
./eng/run-broker-benchmark.ps1 `
    -Client mqttnet `
    -OutputPath perf/baseline/broker-phase4-2026-11-25.json `
    -StartPort 30000
```

该脚本固定使用 QoS `0/1`、订阅者 `1/100`、Payload `32/8192/65536`，
预热 `2s`、测量 `6s`、重复 `1` 次。原始结果保存在
`broker-phase4-2026-11-25.json`；退出码非零表示严格的 MQTTnet 比较门禁
未通过，不代表工具链没有完成采集。

开发矩阵使用同一 12 单元格、重复 `3` 次，结果保存在本地
`artifacts/broker-phase4-dev.json`。

## Phase 0 对比

门限：吞吐不低于 Phase 0 的 `95%`，p99 不高于 Phase 0 的 `105%`。

| QoS | 订阅者 | Payload | 吞吐比 | p99 比 | 结果 |
|---:|---:|---:|---:|---:|:---:|
| 0 | 1 | 32 | 1.26x | 0.50x | PASS |
| 0 | 1 | 8192 | 1.20x | 0.36x | PASS |
| 0 | 1 | 65536 | 1.22x | 0.74x | PASS |
| 0 | 100 | 32 | 0.96x | 0.30x | PASS |
| 0 | 100 | 8192 | 0.88x | 18.04x | FAIL |
| 0 | 100 | 65536 | 1.10x | 0.61x | PASS |
| 1 | 1 | 32 | 1.08x | 0.83x | PASS |
| 1 | 1 | 8192 | 0.98x | 1.08x | FAIL |
| 1 | 1 | 65536 | 1.14x | 0.85x | PASS |
| 1 | 100 | 32 | 0.87x | 0.88x | FAIL |
| 1 | 100 | 8192 | 1.05x | 0.82x | PASS |
| 1 | 100 | 65536 | 0.99x | 0.55x | PASS |

开发门禁结果：`9/12 PASS`。

引发 Phase 4 的核心单元格 `QoS 0 / 100 / 8 KiB`：

- Phase 0：`504.71 msg/s`，p99 `4199.3 us`
- Phase 4 dev：`443.73 msg/s`，p99 `75755.3 us`
- 正式矩阵：`343.20 msg/s`，p99 `115414.0 us`

该单元格吞吐仍低于 Phase 0，p99 在长矩阵和 3 次重复中波动明显，未达到
Phase 4 开发门禁。不能宣称该瓶颈已经修复。

## 严格 MQTTnet 门禁

| QoS | 订阅者 | Payload | MQTTZero msg/s | MQTTnet msg/s | 吞吐比 | MQTTZero p99 us | MQTTnet p99 us | p99 比 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 | 7653.05 | 7375.89 | 1.04x | 263.7 | 239.0 | 1.10x |
| 0 | 1 | 8192 | 6923.08 | 6474.30 | 1.07x | 322.1 | 381.7 | 0.84x |
| 0 | 1 | 65536 | 2738.96 | 3424.04 | 0.80x | 7066.1 | 530.0 | 13.33x |
| 0 | 100 | 32 | 3223.66 | 4176.10 | 0.77x | 939.5 | 1363.4 | 0.69x |
| 0 | 100 | 8192 | 343.20 | 566.95 | 0.61x | 115414.0 | 33240.1 | 3.47x |
| 0 | 100 | 65536 | 51.74 | 236.52 | 0.22x | 276885.3 | 67613.1 | 4.10x |
| 1 | 1 | 32 | 3416.40 | 3581.28 | 0.95x | 658.8 | 467.4 | 1.41x |
| 1 | 1 | 8192 | 3392.96 | 3007.42 | 1.13x | 621.8 | 773.2 | 0.80x |
| 1 | 1 | 65536 | 1873.14 | 2142.96 | 0.87x | 1846.9 | 1354.7 | 1.36x |
| 1 | 100 | 32 | 236.39 | 271.14 | 0.87x | 7768.6 | 8001.7 | 0.97x |
| 1 | 100 | 8192 | 146.84 | 184.33 | 0.80x | 14247.9 | 12381.7 | 1.15x |
| 1 | 100 | 65536 | 42.78 | 51.46 | 0.83x | 64505.1 | 40261.1 | 1.60x |

严格门禁结果：`2/12 PASS`，`10/12 FAIL`。通过的是：

- QoS 0 / 1 / 8 KiB
- QoS 1 / 1 / 8 KiB

未通过项包括所有 100 订阅者单元格、QoS 0 小报文 p99、QoS 0/1 的
64 KiB 单订阅者单元格，以及 QoS 1 单订阅者 32 B。

## 路由与规划微基准

BenchmarkDotNet ShortRun：3 warmup、3 iteration、1 launch。

Route Epoch planning：

| 订阅者 | `#` | `+` | exact |
|---:|---:|---:|---:|
| 1 | 98.03 ns | 108.00 ns | 108.32 ns |
| 100 | 4.52 us | 4.75 us | 8.25 us |
| 1000 | 74.55 us | 80.43 us | 61.26 us |

Route Planning：

| 订阅者 | Shards | 类型 | 平均耗时 |
|---:|---:|---|---:|
| 100 | 1 | regular | 5.45 us |
| 100 | 1 | shared | 2.59 us |
| 100 | 8 | regular | 5.25 us |
| 100 | 8 | shared | 2.61 us |
| 1000 | 1 | regular | 76.89 us |
| 1000 | 1 | shared | 37.68 us |
| 1000 | 8 | regular | 81.78 us |
| 1000 | 8 | shared | 31.42 us |

全部 17 个新增路由/规划微基准均为 `0 B/op`。

## 架构证据

- 每个 Benchmark 过滤器有 2 个 Topic Level；Epoch 更新只复制该路径的
  2 个 Trie 节点和终态 endpoint。1000 个订阅对应 1000 次版本递增。
- `MqttZeroPersistentHashMap` 使用 32-bit hash 和每层 5 bit，HAMT 最多
  7 层；共享组 endpoint 的更新不会复制无关 Trie 子树。
- `CursorSurvivesEquivalentEpochRebuild` 验证共享组轮询游标不嵌入 Trie，
  Epoch 重建后仍延续。
- `OnePublishEnqueuesAtMostOneRemoteWorkPerShard` 验证单发布最多 7 个
  非当前 Shard Work Item。
- `PlanningDoesNotAllocateAfterWarmup` 和
  `SharedPlanningDoesNotAllocateAfterWarmup` 验证普通/共享规划都为零分配。
- `LeaseRemainsAliveUntilRemoteBatchCompletes` 与
  `LeaseIsReleasedWhenRemoteBatchFaults` 验证跨 Shard Lease 生命周期。
- `ConcurrentShardFanoutOwnsBorrowedLeaseBeforeEncoding` 覆盖 borrowed
  multi-segment Lease 在并发 Shard 编码前的 ownership 提升。

## 结论

Phase 4 已删除旧 Route Scope/Snapshot/TopicTrie 路径，并建立不可变 Route
Epoch、持久 HAMT、共享游标、Shard-aware Planner、按目标 Shard 聚合的
Fanout 和 Retained Topic Trie。路由/规划微基准为零分配，且 1000 订阅者
规划在 31–82 us 范围内。

但网络端到端矩阵仍未达到 Phase 0 开发门禁，也未达到 MQTTnet 严格门禁。
当前证据表明剩余瓶颈主要位于 Egress/接收端排空和并发调度，而不是
Planner：无接收端背压的 Outbound 诊断可达约 2100 次发布/秒。后续性能工作
应继续分析 Egress 批处理、Flush 恢复和 ThreadPool 调度，不应宣称 Phase 4
已经通过最终 MQTTnet 门禁。
