# MQTTZero Performance Baselines

`0.3.0-alpha.1`、`0.4.0-alpha.1` 与 `0.4.1-alpha.1` 均使用 BenchmarkDotNet 采集，不作为跨硬件容量承诺。0.4 仅进行目录和 namespace 重构，保留 0.3 结果用于对照。

## 环境

- Windows 11，12th Gen Intel Core i5-12450H，12 逻辑核 / 8 物理核
- .NET SDK 10.0.400
- .NET 10.0.15，X64 RyuJIT x86-64-v3
- BenchmarkDotNet 0.15.8；完整集合使用 `ShortRun`：3 warmup、3 iteration、1 launch
- Header-only 专项使用 `MediumRun`：10 warmup、15 iteration、2 launch
- MQTTnet 5.2.0.1603

## 结论

- `DecodeFast`、`EncodeFast`、`RouteMqttZero` 和内存端到端 `PublishQos0` 均为 `0 B/op`。
- 0.4 的 8 KiB 单段 fast PUBLISH 解码约 `67.99 ns`；MQTTnet 解码约 `434.61 ns`。
- 0.4 的 64 KiB 十段 fast PUBLISH 解码约 `108.54 ns`；MQTTnet 解码约 `3769.45 ns`。
- 0.4 的 8 KiB 内存端到端 QoS 0 路径约 `372.0 ns`，`0 B/op`；同工作负载 MQTTnet decode+match+encode 约 `536.5 ns` 且每次分配约 `8368 B`，当前相对吞吐约 `1.44x`。
- 0.4 的 1/100/1000 订阅者路由分别约 `32.977 ns`、`1767.864 ns`、`18331.680 ns`，均为 `0 B/op`。

## 尚未达成的计划门槛

- MQTTZero 的“将 Payload 复制进目标 Writer”编码与 MQTTnet 的“Payload 以独立 Sequence 返回”不是完全同构；当前 fast encode 的绝对耗时高于 MQTTnet，不能宣称编码 `1.5x`。
- 当前路由在每个消息上展开全部匹配投递候选；与 MQTTnet 仅执行字符串匹配的基线相比，100/1000 全命中场景未达到 `1.5x`。
- 端到端目前使用 MQTTnet packet codec 加 Topic 匹配作为公开可比基线，而不是另一个完整 Broker；`1.2x` 仅在该明确口径下成立。

只有同时满足零分配、性能门槛和稳定性要求的后续版本，才能把上述项目改写为通过。

## 文件

- `broker-phase0-2026-11-25.json`
- `broker-phase0-2026-11-25.md`
- `broker-phase1-2026-11-25.json`
- `broker-phase1-2026-11-25.md`
- `broker-phase2-2026-11-25.json`
- `broker-phase2-2026-11-25.md`
- `broker-phase2-nonblocking-2026-11-25.json`
- `broker-phase2-nonblocking-2026-11-25.md`
- `broker-phase2-official-2026-11-25.json`
- `broker-phase4-2026-11-25.json`
- `broker-phase4-2026-11-25.md`
- `broker-phase4.5-2026-11-25.json`
- `broker-phase4.5-2026-11-25.md`
- `broker-transport-ab-2026-11-25.json`
- `broker-transport-ab-2026-11-25.md`
- `broker-phase4.6-diagnostic-2026-11-25.json`
- `broker-phase4.6-in-progress-2026-11-25.md`
- `broker-phase0-same-machine-2026-11-25.json`
- `broker-phase4.6-2026-11-25.json`
- `broker-phase4.6-2026-11-25.md`
- `broker-phase4.6-scheduled-ack-q1-single-2026-11-25.json`
- `broker-phase4.6-scheduled-ack-q1-100-2026-11-25.json`
- `broker-phase4.6-architecture-review-2026-11-25.md`
- `memory-phase3-2026-11-25.md`
- Phase 0 local development profile: QoS 0/1, 1/100 subscribers, 32 B/8 KiB/64 KiB, 2 s warmup, 6 s measurement and one repetition.
- Phase 0 nightly stable profile: QoS 0/1/2, 1/100 subscribers, 32 B/8 KiB/64 KiB, 5 s warmup, 20 s measurement and three repetitions.
- The 1,000-subscriber cells are deferred until the final performance gate.
- A red Phase 0 performance gate means the toolchain completed and recorded the
  actual result; it does not mean the benchmark infrastructure failed.
- Phase 1 adds event loop shards, removes the Channel Inbound Dispatcher and
  aligns Kestrel IOQueueCount with the logical Shard count.
- Phase 1's 5% regression gate is red in 5 of 12 development cells. The report
  lists each regression; no performance improvement claim is made.
- Phase 2 moves data delivery to a Shard-owned Egress Actor and removes the
  Pump/Writer classes. Its regression gate remains red in 5 of 12 cells.
- The nonblocking Flush revision reduces Phase 2 failures to 4 of 12 cells.
- The write-lock-free revision pools encoded control frames and removes direct
  control writers from attached connections. It also lets Kestrel choose
  PipeWriter segment sizes instead of passing payload-sized hints. Correctness
  and zero-allocation control tests pass; the QoS 0/100/8-KiB development cell
  remains red.
- The Phase 2 official 5x20x3 stability run passes QoS 1/100/8-KiB against the
  Phase 0 tolerance. QoS 0/100/8-KiB retains 97.6% throughput but an 8.72x p99
  regression. Matched counters show lower allocation and GC than MQTTnet, so
  the remaining work moves to Phase 4 aggregated fanout rather than further
  Egress micro-tuning.
- Phase 4 replaces snapshot rebuilds and per-subscriber fanout with immutable
  Route Epochs, persistent HAMTs, stable shared cursors, Shard-aware planning
  and target-Shard fanout batches.
- The Phase 4 development gate passes 9 of 12 cells against Phase 0. The
  strict MQTTnet gate passes 2 of 12 cells. The report keeps those cells red;
  it does not claim the final performance target.
- Phase 4.5 adds QoS 0 Fanout pipelining, low-priority Egress scheduling,
  allocation-free queue insertion fast paths and topology A/B controls. It
  raises QoS 0/100/32-B throughput but still misses the 12-cell development
  gate; Phase 5 remains blocked.
- Phase 3 removes the per-Session QoS 0 candidate array, packs each owned
  message into one Slab and lazily creates Topic Alias storage. Broker private
  memory in the 100-subscriber 8-KiB workload falls from about 308 MB to
  143.6 MB while the Phase 4.5 Egress bottleneck remains.
- Phase 7-Lite adds an internal transport boundary and evaluates an experimental
  SocketAsyncEventArgs TCP transport. The prototype is rejected and removed:
  interleaved repetitions on QoS 0/100/8-KiB show about 11.1% lower throughput
  for Socket despite about 7.8% lower p99. Kestrel remains the only production
  transport; no Socket performance claim is made.
- Phase 4.6 adds a per-Shard Egress Scheduler, Actor-ready deduplication and
  configuration-driven batch limits. The warmup drain stall was traced to a
  lost data signal while `FlushPending`, and the stalled cell now completes.
  The phase remains red: the strict MQTTnet gate passes only 2 of 12 cells,
  and the same-machine Phase 0 development gate passes about 5 of 12.
- QoS 1 now acknowledges after all non-empty Fanout Batches enter their target
  Shard queues while Message Store mode keeps the completion barrier. The
  64-KiB single-subscriber cell improves about 22% in throughput and 23% in
  p99, but remains below the strict MQTTnet gate.
- Persistent per-Shard Fanout Schedulers reuse one high-priority Shard work,
  deduplicate bursts and cap each pass at 250 microseconds. A 1,000-cycle
  release test and the 500-subscriber development matrix pass without
  connection failures or drain timeouts.
- The direct Kestrel `PipeWriter.WriteAsync` experiment for 64-KiB payloads
  was rejected after reducing QoS 1 single-subscriber throughput to about
  0.23x MQTTnet; the production path was restored.
- The Phase 4.6 architecture review identifies the dominant remaining cost as
  the per-message Session/Egress/Flush scheduling chain, with average fanout
  batches near 1.2 messages and almost every QoS 0/1 64-KiB batch waiting on an
  asynchronous Flush. More Encode or batch-constant tuning is not the next step.
- `codec-0.3.0-alpha.1.*`
- `routing-0.3.0-alpha.1.*`
- `memory-transport-0.3.0-alpha.1.*`
- `codec-0.4.0-alpha.1.*`
- `routing-0.4.0-alpha.1.*`
- `memory-transport-0.4.0-alpha.1.*`
- `codec-0.4.1-alpha.1.*`
- `codec-header-medium-0.4.1-alpha.1.*`
- `fanout-0.4.1-alpha.1.*`
- `queue-0.4.1-alpha.1.*`
- `payload-copy-0.4.1-alpha.1.*`
- `routing-0.4.1-alpha.1.*`
- `shared-routing-0.4.1-alpha.1.*`
- `memory-transport-0.4.1-alpha.1.*`

## 0.4.1 优化结果

- Header-only MediumRun 的六个场景均值约 `32.13–35.62 ns`、中位数约 `32.05–33.67 ns`，全部 `0 B/op`。连续 Span 写入消除了旧实现中 8 KiB 单段约 66 ns 的异常退化。
- ShortRun 中 Header-only 对 MQTTnet 的比值约为 `0.89–1.09`。MediumRun 跨运行数据仍显示小报文和 64 KiB 多段场景约 `1.07–1.14` 倍；`≤1.05x` 尚未稳定达成。
- 1000 个相同 Topic Filter 的路由约 `10.19 us`，全部 `0 B/op`，相对 0.4 的约 `18.33 us` 提升约 44%。
- 共享订阅按 1/100/1000 候选分别约 `109.7 ns`、`2.39 us`、`24.48 us`，全部 `0 B/op`，运行时不再创建去重字典或列表。
- QoS 0 的 1/10/100 订阅者扇出约 `182 ns`、`1.48 us`、`16.78 us`；QoS 1 约 `268 ns`、`2.09 us`、`22.72 us`，全部 `0 B/op`。
- 队列深度 1/1000 的完整 Enqueue+Dequeue 分别约 `4.87 ns`、`6.14 us`，全部 `0 B/op`。
- 8 KiB 内存 Transport QoS 0 约 `317.4 ns`，全部 `0 B/op`；同工作负载 MQTTnet codec+match 基线约 `463.7 ns` 且每次分配约 `8368 B`。
- 32 B、8 KiB、64 KiB 的 Payload 复制下界分别约 `2.07 ns`、`56.9 ns`、`1.17 us`。
- 公平 Full Writer ShortRun 为约 `48 / 109 / 1341 ns`（32 B / 8 KiB / 64 KiB），仍未在所有场景优于 MQTTnet，也尚未稳定优于 0.4 的旧 Writer 基线。生产发送路径继续使用已通过协议测试的通用 Encode。
- 控制包属性编码和 CONNACK/DISCONNECT 直接写入使 1000 轮控制包分配哨兵从 `624000 B` 降至 `0 B`。
