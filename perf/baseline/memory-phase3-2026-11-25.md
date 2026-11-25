# Phase 3 Memory Density Baseline

日期：2026-11-25

## 完成内容

### 惰性 QoS 0 候选队列

- 删除每个 Session 的 `Qos0Candidate[MaxMessageQueueLength]`。
- QoS 0 候选改为 `MqttZeroQueuedMessage` 内的侵入式双向链。
- 出队、过期、Clear 和驱逐均在 O(1) 从候选链移除。
- `MqttZeroMessageQueue(1_000_000)` 分配从 `16,000,088 B`
  降至测试上限以下 `< 1024 B`。

### 单 Slab Message Lease

- Owned Lease 固定布局为 `Topic | Properties | Payload`。
- 三块独立池化 Buffer 合并为一个 Slab。
- Borrowed 多段 Sequence 只在 `EnsureOwned()` 时提升一次。
- `Topic`、`Properties`、`Payload` 均指向同一 backing array 的连续区间。
- 释放时仅归还一个 Slab。

### 惰性 Topic Alias

- 删除每连接固定的 257 项 Topic Alias 数组。
- 首次设置 Alias 时才创建小表；大于 256 的 Alias 使用惰性字典。
- Alias 覆盖和 Dispose 均正确归还池化 Topic Buffer。

## 内存收益

同一 QoS 0 / 100 订阅者 / 8 KiB 网络场景：

| 指标 | Phase 4.5 | Phase 3 | 变化 |
|---|---:|---:|---:|
| Broker Private Memory | 约 308 MB | 143.6 MB | -53.4% |
| Broker Peak Working Set | 约 336 MB | 172.7 MB | -48.6% |

该收益主要来自删除每 Session 大候选数组；它不是网络吞吐声明。

## 性能结果

Phase 3 没有显著改善 QoS 0/100/8 KiB 的端到端吞吐。单次顺序测试约
`319 msg/s`，仍低于 Phase 0 的 `504.71 msg/s`。因此：

- Phase 3 的内存密度目标取得实质进展。
- Phase 4.5 的 Egress/网络瓶颈仍然存在。
- Phase 5 继续保持阻塞。

## 未完成项

每 Shard 小型 Buffer Cache 与缓冲池耗尽后的读取暂停/恢复没有加入。
当前没有证据表明它们能解决主要吞吐问题，而且会扩大容量语义和 Shard
生命周期风险。该项留待专用缓存实验。

没有执行 100,000 连接验证，也没有作出容量声明。

## 验证

- Broker 测试：558/558；全量运行中出现一次 WebSocket 200 偶发失败，
  单独复跑通过。
- Dashboard：24/24。
- Benchmark tests：23/23。
- 分配哨兵：21/21。
- MqttZero.Server coverage：84.50% line / 75.30% branch。
- 构建、格式化、CRLF 和 `git diff --check`：通过。
