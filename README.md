# MQTTZero

MQTTZero 是一个面向 ASP.NET Core Kestrel 的独立 MQTT 5 Broker。生产代码不依赖 MQTTnet，目标框架为 .NET 10。当前版本为 `0.11.0-alpha.1`。

## 当前能力

- Kestrel 原始 TCP、TLS 和 MQTT over WebSocket。
- MQTT 5 CONNECT、PUBLISH、QoS 0/1/2、SUBSCRIBE、UNSUBSCRIBE、ACK、PING 和 DISCONNECT。
- 完整 QoS 2 PUBREC/PUBREL/PUBCOMP 握手、重连恢复和 Broker 侧重发去重。
- 持久 Session、Session Expiry、Session Present、客户端 ID 接管和离线 QoS 1/2 队列。
- QoS 1 Inflight `DUP` 重发、KeepAlive 超时、Will/Will Delay 和 Message Expiry。
- UTF-8 Topic 通配符、Topic Alias、共享订阅轮询、Retained 回放和内存 PayloadOwner。
- 支持报文使用强类型 MQTT 5 属性模型，包含 User Property、Content Type、Response Topic、Correlation Data、Payload Format、Reason String 和连接能力属性。
- CONNECT 认证、订阅/退订授权、发布和投递拦截，以及连接与投递 Observer。
- Broker 生命周期、客户端快照和健康检查。
- 活跃传输连接和已完成 CONNECT/CONNACK 的在线客户端分别计数。
- 统一 `MqttZeroPublishMessage` 发布入口，以及 `ReadOnlySequence<byte>`、PipeWriter 工厂和 Stream 便捷扩展。
- Kestrel Accepted Socket、静态 TLS 和 WebSocket 分片策略。
- 池化字节池、引用计数 Payload Lease、无分配 PUBLISH 解码/路由/QoS 1/2 队列/编码和内存 Transport 热路径。
- 可选 Vue 3 + Element Plus 管理面板，构建资源内嵌在 NuGet 程序集中。

## 包结构

- `MqttZero`：客户端与服务端共享的 MQTT 5 协议、发布消息和池化内存基础设施，命名空间包括 `MqttZero.Protocol`、`MqttZero.Publishing` 和 `MqttZero.Memory`。
- `MqttZero.Server`：Broker、Session、路由、拦截器、Observer 和持久化 Store，服务端命名空间统一位于 `MqttZero.Server.*`；配置类型位于根命名空间，客户端、Session 和 Retained 类型分别位于 `Clients`、`Sessions` 和 `Retained`。
- `MqttZero.AspNetCore`：Kestrel TCP、TLS、WebSocket、健康检查和依赖注入入口。
- `MqttZero.AspNetCore.Dashboard`：可选管理面板。

当前尚无客户端 SDK，但公共 `MqttZero` 包已经作为后续客户端的协议和内存边界保留。

## 快速开始

```csharp
using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using MqttZero.AspNetCore;
using MqttZero.AspNetCore.Dashboard;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.Listen(IPAddress.Any, 1883, listen => listen.UseMqttZero(options =>
    {
        options.AcceptedSockets.NoDelay = true;
        options.AcceptedSockets.KeepAlive = true;
    }));
});

builder.Services.AddMqttZero(options =>
{
    options.MaxActiveConnections = 20_000;
    options.ResponseInformation = "mqttzero";
});
builder.Services.AddMqttZeroDashboard(options =>
{
    options.ReadAuthorizationPolicy = "MqttZeroDashboardRead";
    options.OperateAuthorizationPolicy = "MqttZeroDashboardOperate";
});
builder.Services.AddAuthorizationBuilder()
    .AddPolicy("MqttZeroDashboardRead", policy => policy.RequireAuthenticatedUser())
    .AddPolicy("MqttZeroDashboardOperate", policy => policy.RequireRole("MqttAdmin"));

WebApplication app = builder.Build();
app.UseWebSockets();
app.MapMqttZero("/mqtt");
app.MapMqttZeroDashboard("/mqttzero");
await app.RunAsync();
```

完整示例位于 `samples/MqttZero.Sample`。

样板同时监听 HTTP 与 MQTT：

```powershell
dotnet run --project samples/MqttZero.Sample
```

- Dashboard：`http://localhost:5209/mqttzero/`
- 健康检查：`http://localhost:5209/health/ready`
- MQTT：`mqtt://localhost:1883`

端口可通过配置覆盖，例如系统保留了 1883 时：

```powershell
$env:SampleEndpoints__MqttPort = "11883"
$env:SampleEndpoints__HttpPort = "5209"
dotnet run --project samples/MqttZero.Sample
```

`ActiveConnectionCount` 表示已接受、尚未完成清理的 TCP/TLS/WebSocket 连接数，包括尚未完成 CONNECT 的连接；`ConnectedClientCount` 只表示已经成功完成 CONNECT/CONNACK 的在线 MQTT 客户端数。

## 默认资源限制

默认值以 EMQX 6.3.3 为参考，对可安全采用的行为保持一致，同时保留 MQTTZero 的连接、Session、订阅和全局字节预算：

| 配置 | 默认值 |
|---|---:|
| `MaxActiveConnections` | `20_000` |
| `MaxSessions` | `20_000` |
| `MaxSubscriptionsPerSession` | `1_000` |
| `EventLoopShardCount` | `0`，自动使用 `Environment.ProcessorCount` |
| `MaxInflight` | `32` |
| `MaxAwaitingRel` | `100` |
| `MaxMessageQueueLength` | `1_000` |
| `StoreQos0InQueue` | `true` |
| `MaxPacketSize` | `1 MiB` |
| `MaxBufferedBytes` | `512 MiB` |
| `MaxRetainedMessages` | `0`，表示不限数量 |
| `MaxRetainedBytes` | `256 MiB` |
| `ConnectTimeout` | `15 秒` |
| `IncompletePacketTimeout` | `30 秒` |
| `MaxSessionExpiryInterval` | `null`，表示不截断 |
| `KeepAliveCheckInterval` | `500 毫秒` |
| `TopicAliasMaximum` | `65_535` |
| `ResponseInformation` | `null` |

EMQX 默认不对 Listener 连接数和客户端订阅数设置全局上限。MQTTZero 保留有限默认值，避免在缺少进程、容器或反向代理限制时耗尽内存和文件描述符。`KeepAliveCheckInterval` 也保持 `500ms`，因为它同时驱动 Will、Session 和 Retained 清理。

`EventLoopShardCount=0` 时使用逻辑处理器数。CONNECT 后 Session 按 Client ID
稳定映射到 Shard；`AddMqttZero` 默认将 Kestrel `IOQueueCount` 对齐到相同值，
调用方之后显式配置 `SocketTransportOptions` 时可以覆盖。

## 发布 API

业务代码注入 `IMqttZeroBroker`。`PublishAsync` 接收单个消息实体，后续消息字段扩展不会改变核心方法签名：

```csharp
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Broker;
using System.Buffers;

IMqttZeroBroker broker = app.Services.GetRequiredService<IMqttZeroBroker>();
await broker.PublishAsync(
    new MqttZeroPublishMessage
    {
        Topic = "devices/a/state",
        Payload = new ReadOnlySequence<byte>("online"u8.ToArray()),
        QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
        Retain = true,
        Properties = new MqttPublishProperties
        {
            PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8,
            ContentType = "text/plain",
            ResponseTopic = "devices/a/response",
            CorrelationData = "request-42"u8.ToArray(),
            UserProperties =
            [
                new MqttUserProperty("tenant", "north")
            ]
        }
    });
```

`ReadOnlySequence<byte>`、PipeWriter 工厂和 Stream 继续通过 `PublishAsync` 扩展调用：

```csharp
await broker.PublishAsync(
    "devices/a/state",
    new ReadOnlySequence<byte>("online"u8.ToArray()),
    MqttQualityOfServiceLevel.AtMostOnce,
    retain: false);

await broker.PublishAsync(
    "devices/a/state",
    payloadStream,
    MqttQualityOfServiceLevel.AtLeastOnce,
    retain: false);
```

Stream 由调用方拥有；扩展会在发布前将内容收集到池化多段 Payload，并执行最大包大小限制。

端到端消息属性会随普通路由、Retained 回放、Will、离线队列和 QoS 1/2 重发保留。Topic Alias、Subscription Identifier 和 Maximum Packet Size 等连接级属性按目标连接重新处理，不会跨连接原样转发。完整报文与属性矩阵见 [`docs/mqtt5-properties.md`](docs/mqtt5-properties.md)。

CONNECT 的 `Request Problem Information=false` 会禁止 Broker 在控制报文中发送 Reason String 和 User Property；`Request Response Information=true` 时，若配置了 `ResponseInformation`，CONNACK 会返回该属性。Enhanced Authentication 当前不支持，使用 Authentication Method 的 CONNECT 会收到 `Bad Authentication Method`。

具体 `MqttZeroBroker` 仍注册为 singleton，供宿主生命周期和 Kestrel 连接接入使用；普通业务代码应依赖 `IMqttZeroBroker`。

## 拦截器与 Observer

CONNECT 认证、Topic 授权和投递控制使用 DI 拦截器：

```csharp
builder.Services.AddMqttZero()
    .AddConnectionInterceptor<MqttAuthenticator>()
    .AddSubscribeInterceptor<TopicAcl>()
    .AddUnsubscribeInterceptor<TopicAcl>()
    .AddEnqueueInterceptor<DeliveryPolicy>()
    .AddConnectionObserver<PresenceObserver>()
    .AddDeliveryObserver<DeliveryMetricsObserver>();
```

`IMqttZeroConnectionInterceptor` 可返回 `Continue`、`Reject` 或 `Drop`；拒绝时通过 `RejectReasonCode` 返回 MQTT 5 CONNACK 原因。订阅和退订授权按 Topic Filter 执行，被拒绝的 Filter 不进入路由快照。

Observer 只观察、不参与 Allow/Deny 决策，异常会被隔离。用户名和客户端证书可提供给扩展；原始密码只在 CONNECT 拦截上下文生效，不进入 Observer。启用 Observer 后允许分配，未注册扩展时原有热路径继续执行零分配检查。

连接成功后，Observer 会获得连接期有效的 `MqttZeroConnectedClient`：

```csharp
public async ValueTask OnConnectedAsync(
    MqttZeroClientConnectedContext context,
    CancellationToken cancellationToken)
{
    context.ConnectedClient.Items["role"] = "device";
    await context.ConnectedClient.SubscribeAsync(
        [new MqttSubscriptionRequest
        {
            TopicFilter = new ReadOnlySequence<byte>("commands/#"u8.ToArray())
        }],
        cancellationToken: cancellationToken);
}
```

句柄支持服务器代订阅、退订、`SendPublishAsync`、断开、Session Items、状态快照和统计。`SendPublishAsync` 只向目标客户端发送 PUBLISH，不按 Topic 路由，也不更新 Retained Store；按 Topic 路由仍使用 Broker `PublishAsync`。服务器代订阅不会向客户端发送 SUBACK。句柄在断线或 Session 接管后失效。

连接确定在线时，拦截器和 Observer 只提供 `MqttZeroConnectedClient`，身份信息通过 `ConnectedClient.ClientInfo` 获取。消息接收者可能离线时，上下文同时提供始终存在的 `ReceiverInfo` 和在线时才非空的 `Receiver`；离线场景只能读取 `ReceiverInfo`。

## 持久化 Store

Broker 提供三个独立持久化接口：

- `IMqttZeroSessionStore`
- `IMqttZeroMessageStore`
- `IMqttZeroRetainedMessageStore`

默认注册的是无操作 Store，支持同一进程内的断线重连，但不支持进程重启恢复，也不复制运行时 Payload。替换任意 Store 即可接入外部持久化：

```csharp
builder.Services.AddSingleton<IMqttZeroSessionStore, MySessionStore>();
builder.Services.AddSingleton<IMqttZeroMessageStore, MyMessageStore>();
builder.Services.AddSingleton<IMqttZeroRetainedMessageStore, MyRetainedMessageStore>();
builder.Services.AddMqttZero();
```

Store 采用严格 Write-Through：Save/Delete 成功后才发送对应 ACK；失败时 Broker 回滚内存状态并关闭相关连接。Message Store 的 `TransitionQueuedToOutboundAsync` 必须原子地把 queued 记录替换为 outbound 记录，queued、outbound 和过期消息统一通过 `DeleteMessageAsync` 删除。外部 Store 在保存二进制字段时必须复制数据，不能引用 Broker 的池化 Lease。默认无操作模式仍执行零分配热路径检查。

Dashboard 的客户端和 Session 列表支持 `page`、`pageSize` 参数，默认值和上限由 `MqttZeroDashboardOptions` 控制。

## Kestrel 入口配置

`UseMqttZero` 支持连接级 TCP 选项和静态 TLS。Listening Socket 的 `ReuseAddress` 等选项仍由 Kestrel `SocketTransportOptions` 管理。

```csharp
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

kestrel.Listen(IPAddress.Any, 8883, listen => listen.UseMqttZero(options =>
{
    options.AcceptedSockets.KeepAlive = true;
    options.Tls = new MqttZeroTlsOptions
    {
        ServerCertificate = certificate,
        SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
    };
}));
```

证书在监听器启动时使用，不提供热替换。WebSocket 端点可按连接选择是否允许 MQTT PUBLISH 分片：

```csharp
app.MapMqttZero("/mqtt", options =>
{
    options.AllowPacketFragmentationSelector = transport =>
        !transport.IsSecure;
});
```

WebSocket 未设置选择器时默认不允许分片，从而保持 MQTT PUBLISH 单帧语义；TCP/TLS 端点未设置选择器时仍允许分片。

## 性能与分配

性能与分配约束覆盖 Broker 稳态同步热路径：

- `MqttPublishPacketCodec` 直接解析 `ReadOnlySequence<byte>`，不创建完整报文或 Topic 数组。
- Topic/Properties/Payload 由引用计数 `MqttZeroMessageLease` 共享；在线订阅者、离线队列和 Inflight 消息持有同一份内存。
- 路由使用不可变 Route Epoch、路径级 Copy-on-Write Topic Trie 和持久 HAMT；
  发布在发布者 Shard 规划，并按目标 Shard 聚合成单次 Fanout Batch。
- QoS 1/2 描述符、确认包和出站 PUBLISH 编码复用池化对象，并直接写入连接自己的 `IBufferWriter`。
- `MqttZeroBufferPool` 以 `ArrayPool<byte>` 为底层来源执行字节预算；耗尽时暂停读取并等待释放，不静默回退到普通数组。

零分配口径仅包含预热后的单线程同步 Broker 核心热路径。Kestrel/TLS/WebSocket、连接建立、订阅变更、Retained 更新、管理 API、日志、异常、Observer 和异步拦截器续接允许分配。WebSocket 为维持单帧语义允许在边界复制一次 Payload。

运行分配哨兵：

```powershell
./perf/verify-allocation.ps1
```

运行完整基准：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.Benchmarks -- `
  --filter '*' --job short --exporters csv json
```

当前基准结果、机器信息和解释统一记录在 `perf/baseline`。fast PUBLISH
解码、Header 编码、Route Epoch、Route Planning、QoS 1 队列、Retained
Trie 和内存端到端均经过 `0 B/op` 分配验证；网络 Broker 对比结果和尚未
通过的单元格以 baseline 报告为准。

Phase 4.5 的 QoS 0 Fanout 流水化、Egress 低优先级调度和拓扑 A/B 结果见
`perf/baseline/broker-phase4.5-2026-11-25.md`。当前网络热路径门禁仍未
全部通过，Phase 5 暂缓。

Phase 4.6 已修复 Egress 控制包与数据 Flush 的状态竞态，以及
FlushPending 期间新数据信号丢失的问题。12 格三次矩阵现在可以完整运行，
但严格 MQTTnet 门禁仍只通过 2/12，单订阅 64 KiB 的跨 Shard 尾延迟仍是
主要阻塞；阶段保持红色，详见
`perf/baseline/broker-phase4.6-2026-11-25.md`。

Phase 3 已将 QoS 0 候选队列改为完全惰性侵入链、消息 Lease 合并为单
Slab，并按需创建 Topic Alias 表。内存结果见
`perf/baseline/memory-phase3-2026-11-25.md`。

Phase 7-Lite 已用同一 Broker 核心对 Kestrel 与实验
`SocketAsyncEventArgs` TCP 传输做交替重复 A/B。实验传输吞吐中位数更低，
未同时改善吞吐和 p99，因此已删除；Kestrel 继续作为唯一生产传输。记录见
`perf/baseline/broker-transport-ab-2026-11-25.md`。

全局 Packet Writer 池经顺序和 128 并发基准验证后决定不引入，结论见 `perf/baseline/packet-writer-pool-0.5.0-alpha.1.md`。完整 Broker 对 Broker 的 TCP/QoS/多连接比较方法见 `perf/baseline/broker-comparison-methodology.md`。

## 构建与测试

```powershell
dotnet build MqttZero.slnx -c Release
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release
./eng/verify-coverage.ps1
```

Vue 源码构建：

```powershell
Set-Location dashboard/MqttZero.Dashboard
npm ci
npm run build
```

构建后的 Vue 资源会写入 `src/MqttZero.AspNetCore.Dashboard/wwwroot` 并嵌入 Dashboard 程序集；运行时消费者不需要 Node。

## 安全

- Dashboard 默认不上线，必须显式 `MapMqttZeroDashboard` 并配置读写授权策略。
- 写操作要求 Antiforgery Token。
- 日志不得记录密码、认证数据或完整 Payload。
- 示例中的放行策略只用于本地体验，生产部署必须替换为真实身份与角色策略。

## 限制

当前不包含 Enhanced Authentication/AUTH 流程、自动出站 Topic Alias、MQTT 3.1.1、具体数据库 Store、集群、桥接和客户端 SDK。默认 Noop Store 下 Session、Will、QoS 1/2 队列和 QoS 2 握手状态在进程重启后丢失；配置外部 Store 后由 Store 提供恢复能力。Dashboard、管理 API、异步拦截器续接和网络框架边界不属于 `0 B/op` 承诺。Dashboard Live Events 仍未实现。
