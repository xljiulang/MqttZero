# 0.12 公开接口变更与迁移

> 上级索引：[文档目录](README.md)

0.12.0-alpha.1 是一次**中断式**变更：没有兼容层，也没有 `[Obsolete]` 过渡。本文是升级指引。

## 1. 协议编解码与报文模型收进 assembly

以下类型不再是 `public`，外部代码不能再直接引用：

`MqttPacket`、`MqttConnectPacket`、`MqttConnAckPacket`、`MqttPublishPacket`、`MqttSubscribePacket`、`MqttSubAckPacket`、`MqttUnsubscribePacket`、`MqttUnsubAckPacket`、`MqttPubAckPacket`、`MqttPubRecPacket`、`MqttPubRelPacket`、`MqttPubCompPacket`、`MqttPingReqPacket`、`MqttPingRespPacket`、`MqttDisconnectPacket`、`MqttPacketCodec`、`MqttPacketType`。

仍然公开（出现在公开签名里）：`MqttQualityOfServiceLevel`、`MqttReasonCode`、`MqttPublishProperties`、`MqttUserProperty`、`MqttSubscriptionRequest`、`MqttRetainHandling`、`MqttProtocolVersion`、`MqttPayloadFormatIndicator`。

**迁移**：需要读取或构造 MQTT 报文的场景，改用 Broker 的公开事件与拦截器；如果确实需要编解码能力，请提出需求，后续会设计一个稳定的公开门面，而不是重新暴露报文类。

## 2. 连接拦截器改为接收请求快照

| 0.11 | 0.12 |
|---|---|
| `MqttZeroConnectionContext.ConnectPacket.ClientId` | `MqttZeroConnectionContext.Request.ClientId` |
| `ConnectPacket.UserName`（`ReadOnlySequence<byte>` + `HasUserName`） | `Request.UserName`（`string?`，未提供为 `null`） |
| `ConnectPacket.Password` | `Request.Password`（`ReadOnlySequence<byte>`，未提供为空） |
| `ConnectPacket.CleanStart` / `KeepAliveSeconds` / `SessionExpiryInterval` | 同名属性在 `Request` 上 |
| `ConnectPacket.ReceiveMaximum` / `MaximumPacketSize` | 同名属性在 `Request` 上 |
| `ConnectPacket.RequestProblemInformation` / `RequestResponseInformation` | 同名属性在 `Request` 上 |
| `ConnectPacket.HasWill` / `WillTopic` / `WillPayload` / `WillQualityOfServiceLevel` / `WillRetain` / `WillDelayInterval` / `WillProperties` | `Request.Will`：`null` 表示没有遗嘱；否则是 `MqttZeroWillRequest`，含 `Topic`、`Payload`、`QualityOfServiceLevel`、`Retain`、`DelayInterval`、`Properties` |

`MqttZeroConnectionContext.Password` 便捷属性保留，值来自 `Request.Password`。

## 3. 查询方法改名

| 0.11 | 0.12 |
|---|---|
| `IMqttZeroBroker.GetClients()` | `GetClientSnapshots()` |
| `IMqttZeroBroker.GetSessions()` | `GetSessionSnapshots()` |
| `IMqttZeroBroker.GetRetainedMessages()` | `GetRetainedMessageSnapshots()` |

名字明确表示返回的是**某一刻的快照**：逐项构造 DTO，不保证跨分片原子一致，也不持有内部对象引用。返回类型与同步语义不变。

## 4. 单客户端发送方法改名

| 0.11 | 0.12 |
|---|---|
| `MqttZeroConnectedClient.SendPublishAsync(...)` | `MqttZeroConnectedClient.TrySendPublishAsync(...)` |

返回 `ValueTask<bool>`：客户端已断开时返回 `false`，不抛异常。
