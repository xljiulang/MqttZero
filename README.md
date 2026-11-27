# MQTTZero

MQTTZero 是直接运行在 ASP.NET Core / Kestrel 上的 MQTT 5 Broker：设备接入与现有 HTTP 服务
同进程部署，不需要再单独维护一个 Broker 进程。MIT 许可，目标框架 .NET 10，当前版本
`0.12.0-alpha.1`（alpha）。

## 为什么把它放进现有服务

- **一个进程，一个部署单元。** MQTT 与 HTTP/WebSocket 共用同一份依赖注入、配置、日志、
  健康检查和发布流程；不再有独立的 Broker 进程、跨进程鉴权和第二套运维脚本。
- **业务代码直接收发。** 服务端通过 `IMqttZeroBroker` 主动下发消息，与设备上报走同一套
  路由、QoS 和离线队列，不需要为控制指令另建一条通道。
- **策略在进程内执行。** 连接认证、Topic 订阅授权、发布拦截和投递策略都是同进程的
  拦截器与 Observer，可以直接读取宿主应用的用户、租户和角色，不需要桥接或反向调用。
- **资源有界。** 连接数、会话数、订阅数、待发队列和全局字节预算都有明确上限；
  预算耗尽时暂停读取并等待归还，而不是静默丢弃或无限增长。
- **协议完整，传输原生。** MQTT 5 全流程（QoS 0/1/2、持久会话、Will、Retained、
  Message Expiry、消息属性、共享订阅、Topic Alias），Kestrel 原生 TCP、TLS 与 WebSocket。

## 典型场景

- 已有 ASP.NET Core 服务需要接入设备，但不想为 MQTT 再引入一套独立中间件。
- 服务端需要主动下发指令、配置或固件通知，并要求与设备上报共享同一套授权与审计。
- 内网或私有化部署，要求依赖少、可审计、可随宿主应用一起打包与升级。

## 不适用场景

- 需要集群、跨机房桥接或大规模水平扩展（当前是单机 Broker）。
- 需要 MQTT 3.1.1 或 MQTT 5 增强认证（AUTH）流程。
- 需要开箱即用的数据库持久化 Store（当前只提供接口与内存实现）。

## 成熟度与许可

项目处于 alpha：协议与 API 仍可能调整，默认持久化为进程内实现，重启后会话与队列不保留。
仓库包含协议、服务端与宿主的完整回归测试、真实 TCP/TLS/WebSocket 测试、分配哨兵与覆盖率门禁。
许可为 MIT，问题反馈请使用 GitHub Issues。

[开发者从这里开始](#开发者指南)

## 开发者指南

### 快速开始

```csharp
using System.Net;
using MqttZero.AspNetCore;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.WebHost.ConfigureKestrel(kestrel =>
    kestrel.Listen(IPAddress.Any, 1883, listen => listen.UseMqttZero()));
builder.Services.AddMqttZero();

WebApplication app = builder.Build();
app.UseWebSockets();
app.MapMqttZero("/mqtt");
await app.RunAsync();
```

完整示例（同时监听 HTTP 与 MQTT，并挂载 Dashboard 与健康检查）：

```powershell
dotnet run --project samples/MqttZero.Sample
```

- MQTT：`mqtt://localhost:1883`
- Dashboard：`http://localhost:5209/mqttzero/`
- 就绪检查：`http://localhost:5209/health/ready`

端口可用配置覆盖，例如系统已占用 1883 时设置
`$env:SampleEndpoints__MqttPort = "11883"` 再启动示例。

### 项目与命名空间

| 项目 | 职责 | 命名空间 |
|---|---|---|
| `MqttZero` | MQTT 5 协议、发布消息与池化内存基础设施 | `MqttZero.Protocol`、`MqttZero.Publishing`、`MqttZero.Memory` |
| `MqttZero.Server` | Broker、会话、路由、拦截器、Observer 与持久化 Store | `MqttZero.Server.*` |
| `MqttZero.AspNetCore` | Kestrel TCP/TLS/WebSocket 接入、健康检查与依赖注入入口 | `MqttZero.AspNetCore` |
| `MqttZero.AspNetCore.Dashboard` | 可选管理面板（Vue 3 资源内嵌在程序集中） | `MqttZero.AspNetCore.Dashboard` |

当前通过源码引用（项目引用）使用，仓库尚未提供独立的客户端 SDK。

### 架构概览

设备报文进入 Kestrel 后，由连接读循环在所属分片上解码与路由；会话按 Client ID 稳定映射到
分片，路由读取不可变快照，出站消息经每连接的 Egress Actor 组批编码后写入 `PipeWriter`。
消息本体由引用计数的 `MqttZeroMessageLease` 共享，底层字节来自有预算的
`MqttZeroBufferPool`。完整说明见 [`docs/architecture/runtime.md`](docs/architecture/runtime.md)。

### 关键配置

完整配置项与含义见 `MqttZeroOptions` 上的 XML 注释，常用默认值如下：

| 配置 | 默认值 | 说明 |
|---|---:|---|
| `MaxActiveConnections` | `20_000` | 已接受、尚未清理的传输连接上限 |
| `MaxSessions` | `20_000` | 存储的会话上限 |
| `MaxSubscriptionsPerSession` | `1_000` | 单会话订阅上限 |
| `EventLoopShardCount` | `0` | `0` 表示逻辑处理器数 |
| `MaxInflight` | `32` | 单会话未确认的 QoS 1/2 出站上限 |
| `MaxMessageQueueLength` | `1_000` | 单会话待发队列上限（`StoreQos0InQueue` 默认 `true`） |
| `OutboundBatchMessages` / `OutboundBatchBytes` / `OutboundBatchDuration` | `128` / `512 KiB` / `500 µs` | 单批出站组批预算 |
| `MaxPacketSize` | `1 MiB` | 接受的单个 MQTT 报文上限 |
| `MaxBufferedBytes` | `512 MiB` | 池化字节总预算 |
| `MaxRetainedMessages` / `MaxRetainedBytes` | `0`（不限数量）/ `256 MiB` | Retained 预算 |
| `ConnectTimeout` / `IncompletePacketTimeout` | `15 s` / `30 s` | CONNECT 与半包等待上限 |
| `KeepAliveCheckInterval` | `500 ms` | 同时驱动 KeepAlive、Will、Session 与 Retained 到期 |

`EventLoopShardCount=0` 表示使用逻辑处理器数（上限 256）；该值只影响 Broker 分片，
Kestrel 的 `IOQueueCount` 仍由宿主按 `SocketTransportOptions` 自行配置。

`ActiveConnectionCount` 表示已接受、尚未完成清理的 TCP/TLS/WebSocket 连接数（含尚未完成
CONNECT 的连接）；`ConnectedClientCount` 只表示已完成 CONNECT/CONNACK 的在线客户端数。

### 发布与下发

注入 `IMqttZeroBroker` 后按 Topic 路由发布消息：

```csharp
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Broker;
using System.Buffers;

IMqttZeroBroker broker = app.Services.GetRequiredService<IMqttZeroBroker>();
await broker.PublishAsync(new MqttZeroPublishMessage
{
    Topic = "devices/a/state",
    Payload = new ReadOnlySequence<byte>("online"u8.ToArray()),
    QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
    Retain = true,
    Properties = new MqttPublishProperties { ContentType = "text/plain" }
});
```

`ReadOnlySequence<byte>`、PipeWriter 工厂与 Stream 通过同名扩展方法发布；Stream 由调用方拥有，
内容会在发布前收集到池化多段 Payload 并校验最大包大小。消息属性会随路由、Retained 回放、
Will、离线队列与 QoS 1/2 重发保留，完整矩阵见
[`docs/protocol/mqtt5-properties.md`](docs/protocol/mqtt5-properties.md)。

“按 Topic 路由”和“向单个客户端发送”是两件事：前者用 `IMqttZeroBroker.PublishAsync`，
经过订阅匹配、Retained 与离线队列；后者用 `MqttZeroConnectedClient.SendPublishAsync`，
只向该连接写一条 PUBLISH，不路由、不更新 Retained。

### 扩展点

```csharp
builder.Services.AddMqttZero()
    .AddConnectionInterceptor<MqttAuthenticator>()
    .AddSubscribeInterceptor<TopicAcl>()
    .AddUnsubscribeInterceptor<TopicAcl>()
    .AddEnqueueInterceptor<DeliveryPolicy>()
    .AddConnectionObserver<PresenceObserver>()
    .AddDeliveryObserver<DeliveryMetricsObserver>();
```

- 拦截器参与决策（连接可 `Continue`/`Reject`，订阅、退订、发布与入队可拒绝或消费）；
  Observer 只观察，异常被隔离，不能改变投递结果。
- 连接建立后可用 `MqttZeroConnectedClient` 代订阅、退订、发送、断开并读写 `Items`，
  句柄在断线或会话接管后失效。
- 持久化接 `IMqttZeroSessionStore`、`IMqttZeroMessageStore`、`IMqttZeroRetainedMessageStore`：
  写穿语义，Save/Delete 成功后才 ACK，失败回滚内存并关闭连接；外部 Store 必须复制二进制字段。

### 传输与安全

```csharp
kestrel.Listen(IPAddress.Any, 8883, listen => listen.UseMqttZero(options =>
{
    options.Tls = new MqttZeroTlsOptions
    {
        ServerCertificate = certificate,
        SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
    };
}));
```

- 证书在监听器启动时使用，不支持热替换。
- WebSocket 默认禁止 PUBLISH 分片以保持单帧语义（`AllowPacketFragmentationSelector` 可按连接放开），
  TCP/TLS 默认允许分片。
- Dashboard 必须显式 `MapMqttZeroDashboard` 并配置读写授权策略，写操作要求 Antiforgery Token；
  日志不得记录密码、认证数据或完整 Payload。

### 性能与分配

发布热路径按池化内存、共享消息租约、不可变路由快照与组批出站设计，在线订阅、离线队列与
Inflight 共享同一份 Payload：

```powershell
./perf/verify-allocation.ps1
dotnet run -c Release --project benchmarks/MqttZero.Benchmarks -- --filter '*' --job short
```

零分配口径只覆盖预热后的单线程同步 Broker 核心热路径；Kestrel/TLS/WebSocket、连接建立、
订阅变更、Retained 更新、管理 API、日志、异常、Observer 与异步拦截器续接允许分配。
端到端对比与门禁口径见 [`docs/performance/positioning.md`](docs/performance/positioning.md)
与 [`perf/baseline/README.md`](perf/baseline/README.md)。

### 构建与测试

```powershell
dotnet build MqttZero.slnx -c Release
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release
./eng/verify-coverage.ps1
```

Dashboard 的 Vue 源码需要单独构建，产物写入 `src/MqttZero.AspNetCore.Dashboard/wwwroot`
并嵌入程序集，运行时消费者不需要 Node：

```powershell
Set-Location dashboard/MqttZero.Dashboard
npm ci
npm run build
```

代码规范、覆盖率门禁与提交流程见 [`CONTRIBUTING.md`](CONTRIBUTING.md)。

### 已知限制

- 不支持 MQTT 5 增强认证（AUTH）、自动出站 Topic Alias、MQTT 3.1.1、集群与桥接，也不提供客户端 SDK。
- 只提供持久化接口与内存实现：默认 Noop Store 下会话、Will、QoS 1/2 队列与 QoS 2 握手状态
  在进程重启后丢失；Dashboard 的 Live Events 尚未实现。
- Egress 发送闭环仍在收敛中；每分片时间轮与分片去锁已完成，见
  [`docs/architecture/runtime.md`](docs/architecture/runtime.md)。

### 文档索引

- [`docs/README.md`](docs/README.md)：文档目录（架构 / 协议 / 性能 / 历史记录）。
- [`docs/architecture/runtime.md`](docs/architecture/runtime.md)：运行时结构、内存所有权、已否决方案与未完成项。
- [`docs/protocol/mqtt5-properties.md`](docs/protocol/mqtt5-properties.md)：报文与属性的端到端矩阵。
- [`docs/performance/positioning.md`](docs/performance/positioning.md)：性能定位与发布门禁。
- [`docs/performance/ab-harness.md`](docs/performance/ab-harness.md)：同机 A/B 基准口径。
- [`perf/baseline/README.md`](perf/baseline/README.md)：历史基准记录索引。
- [`CONTRIBUTING.md`](CONTRIBUTING.md)：代码规范与开发流程。
