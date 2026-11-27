# MQTTnet Fork 优化项与 MQTTZero 对照审计

> 上级索引：[文档目录](../README.md)

记录日期：2026-11-24

## 当前基线

- MQTTnet fork：`D:\github\MQTTnet`
- MQTTnet fork 远端：https://github.com/xljiulang/MQTTnet
- MQTTZero 本地仓库：`D:\github\mqttzero`
- MQTTZero 远端：https://github.com/xljiulang/MqttZero
- 审计时 MQTTZero `master`：`4d85771`
- MQTTZero 版本：`0.5.0-alpha.1`
- 审计时工作树干净，本地与远端 `master` SHA 一致

性能基准位于 `perf/baseline`。最近的完整基准基于 `0.4.1-alpha.1`；
`0.5.0-alpha.1` 只完成 Endpoint 和协议有限值强类型化，没有修改消息热路径，
因此当前性能判断仍以 `0.4.1-alpha.1` 数据为主要依据。

## MQTTnet fork 分支主题

| 分支 | 主要优化或能力 |
|---|---|
| `buffer-reader` | Codec、BufferReader/Writer、Span、ReadOnlyMemory、PayloadOwner、PacketInspector、零分配发送 |
| `Refactoring` / `asp_net` | Kestrel/AspNetCore 接入、TCP 选项、TLS、WebSocket、服务注册和测试宿主 |
| `master` | Accepted Socket、IPv4/IPv6 DualMode、RemoteEndPoint、WebSocket MQTT 子协议、分片选项 |
| `websocket-feature` | `AllowPacketFragmentationSelector` |
| `sessions-manager` | 入队前复制 Publish Packet 的正确性问题 |
| `SslProtocol` | SSL 协议默认值和文档 |
| `Directory.Build` | 分析器、确定性构建和 CI 属性 |
| `MessageProcessingBenchmark` | 消息处理性能基准 |
| `server-event-arg` | Server EventArgs 暴露 UserName |

## 已体现在 MQTTZero 的优化

| MQTTnet fork 优化 | MQTTZero 实现 |
|---|---|
| `MqttBufferReader` 减少分配、优化多 Segment 读取 | 使用 `SequenceReader<byte>` 和 `ReadOnlySequence<byte>` 零拷贝解码 |
| `DecodePublishPacket` 减少分配 | `MqttPublishPacketCodec` 直接返回 Topic、Properties、Payload 切片 |
| `ReadOnlyMemory` / `Span` 替代不必要的 `byte[]` | Codec、Packet、路由和 Writer 广泛使用 `ReadOnlySequence`、`ReadOnlyMemory`、`Span` |
| `MqttBufferWriter` 使用 `GetSpan` | `MqttZeroPacketWriter` 直接写 Span，整数使用 `BinaryPrimitives` |
| Header/Payload 分离 | reserved-header/backfill，Header 和 Payload Sequence 分离 |
| `MqttPayloadOwnerFactory` | `MqttZeroPayloadOwnerFactory` 支持 `ArrayPool` 单段 Payload 和 Pooled Pipe 多段 Payload |
| Payload factory 和 CancellationToken | 已支持 payload callback 和异步取消 |
| `SendPacketAsync` 减少复制和 Flush | 控制包和 PUBLISH 直接写连接 `PipeWriter`，通常只 Flush 一次 |
| Retained 消息复制语义 | Retained Store 对共享 `MqttZeroMessageLease` 执行一次 `EnsureOwned()`，之后引用计数共享 |
| 入队前复制 Publish Packet | QoS 1/离线队列提升并共享 Lease，避免原始输入被回收后失效 |
| `RemoteEndPoint` 强类型 | `MqttZeroClientSnapshot.RemoteEndPoint` 为 `EndPoint?` |
| Kestrel `UseMqtt` / `MapMqtt` | `UseMqttZero()` 和 `MapMqttZero()`，WebSocket 限定 `mqtt` 子协议 |
| 构建分析和确定性构建 | `Directory.Build.props` 开启分析器、警告即错误、确定性构建 |
| Message Processing Benchmark | MQTTZero 已有 Codec、Routing、Fanout、Queue、Payload Copy 和 Memory Transport 基准 |

关键实现：

- `src/MqttZero.Server/Protocol/MqttPublishPacketCodec.cs`
- `src/MqttZero.Server/Protocol/MqttZeroPacketWriter.cs`
- `src/MqttZero.Server/MqttZeroPayloadOwnerFactory.cs`
- `src/MqttZero.Server/Memory/MqttZeroMessageLease.cs`
- `src/MqttZero.Server/MqttZeroClientConnection.cs`
- `src/MqttZero.AspNetCore/MqttZeroConnectionHandler.cs`

## 部分体现

### Buffer Writer Pool

- MQTTZero 每条连接复用一个 `MqttZeroPacketWriter`。
- 尚未实现 MQTTnet fork 中跨连接的 `MqttBufferWriterPool` 和 Pool Options。
- 当前状态是“连接内复用”，不是“连接间池化”。

### Unix Domain Socket

- Kestrel 可以监听 Unix Domain Socket，Broker 也已使用强类型 `EndPoint?`。
- 尚无 MQTTZero 专用监听扩展、示例和端到端 Unix Socket 测试。

### TLS

- TCP/TLS/WebSocket 由 Kestrel 承载。
- MQTTZero 没有独立的 `SslProtocol`、TLS Endpoint Options、证书提供器或证书热替换 API。
- 当前依赖宿主直接配置 Kestrel TLS。

### 性能对照

- 已有 MQTTZero 与 MQTTnet Codec/Topic Matcher 的同机比较。
- 尚无完整 MQTTnet Broker 与 MQTTZero Broker 的公平 TCP 压力测试。

## 尚未体现

### Accepted Socket 选项

未提供 MQTTnet fork 中的 Accepted Socket 适配，包括：

- `NoDelay`
- TCP KeepAlive
- `LingerState`
- IPv4/IPv6 `DualMode`
- Accepted Socket 与 Listening Socket 的 `ReuseAddress` 分离

当前 `UseMqttZero()` 仅挂载 `ConnectionHandler`，Socket 选项需用户直接配置 Kestrel。

### Packet Inspector

未实现：

- `MqttPacketInspector`
- Fill/FillReceiveBuffer 状态
- 用嵌入 Pipe 降低检查器分配

### WebSocket Fragmentation

未实现 `AllowPacketFragmentationSelector` 或等价的 WebSocket 分片策略。

### 高性能流式发布

未实现 MQTTnet fork 中的：

- `PublishStreamAsync`
- `InjectStreamAsync`
- 高性能流式 Inject 扩展

MQTTZero 当前只有内存 Payload 的 `PublishAsync`。

### Server EventArgs UserName

CONNECT Packet 能解析 UserName，但 MQTTZero 尚未提供 Connect/Disconnect 事件参数 API。
发布拦截器当前只暴露 `SenderClientId`。

### 客户端相关优化

以下 MQTTnet 客户端优化不适用于当前 MQTTZero，因为 MQTTZero 不提供客户端 SDK：

- `PublishStringAsync` 零分配
- Client RPC 分配优化
- Client Adapter Factory
- JSON Payload 转换优化

## 当前性能结论

基准文件：`perf/baseline/README.md`

- 8 KiB 单段 PUBLISH 解码：MQTTZero 约 `91 ns`，MQTTnet 约 `524 ns`。
- 64 KiB 单段 PUBLISH 解码：MQTTZero 约 `81 ns`，MQTTnet 约 `3041 ns`。
- 8 KiB 内存 Transport：MQTTZero 约 `317 ns`，MQTTnet codec+match 基线约 `464 ns`。
- Header-only：MQTTZero 约 `32–34 ns`，MQTTnet 约 `30–32 ns`。
- 公平 Full Writer：MQTTZero 约 `48 / 109 / 1341 ns`（32 B / 8 KiB / 64 KiB），当前部分场景慢于 MQTTnet。
- 1000 相同 Filter 路由：MQTTZero 约 `10.19 us`，全部 `0 B/op`。

注意：路由基准中 MQTTnet 只做字符串匹配，MQTTZero 同时生成和去重投递候选，
不是完全同构比较。

## 下一阶段建议优先级

1. Kestrel Accepted Socket 选项适配和配置扩展。
2. 对全局 Packet Writer Pool 做基准验证后决定是否引入。
3. WebSocket 分片策略。
4. 高性能 `PublishStreamAsync` 和流式 Inject。
5. TLS 配置与证书热替换抽象。
6. Unix Domain Socket 示例和端到端测试。
7. 完整 MQTTnet Broker 对 MQTTZero Broker 的 TCP/QoS/多连接公平压力测试。

不建议直接移植 Packet Inspector、客户端 RPC/JSON 和 PublishString，
除非后续明确扩展 MQTTZero 的客户端或报文检查能力。

## 换电脑继续

```powershell
git clone https://github.com/xljiulang/MqttZero.git D:\github\mqttzero
Set-Location D:\github\mqttzero
dotnet restore MqttZero.slnx --locked-mode
dotnet build MqttZero.slnx -c Release --no-restore
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-build
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release --no-build
```

MQTTnet fork 需要另外 clone：

```powershell
git clone https://github.com/xljiulang/MQTTnet.git D:\github\MQTTnet
```

继续工作时先重新检查 MQTTZero 最新 `master` 和远端 CI 状态。
