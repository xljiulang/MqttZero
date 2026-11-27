# MQTT 5 属性支持矩阵

> 上级索引：[文档目录](../README.md)

MQTTZero 对当前支持的报文执行属性类型、重复次数、取值范围、UTF-8、Topic Name、报文方向和数据流语义校验。未知属性或出现在错误报文中的属性会被视为协议错误。

## 报文属性

| 报文 | 支持的属性 |
|---|---|
| CONNECT | Session Expiry Interval、Receive Maximum、Maximum Packet Size、Topic Alias Maximum、Request Response Information、Request Problem Information、User Property、Authentication Method、Authentication Data |
| Will Properties | Will Delay Interval、Payload Format Indicator、Message Expiry Interval、Content Type、Response Topic、Correlation Data、User Property |
| CONNACK | Session Expiry Interval、Receive Maximum、Maximum QoS、Retain Available、Maximum Packet Size、Assigned Client Identifier、Topic Alias Maximum、Reason String、User Property、Wildcard Subscription Available、Subscription Identifier Available、Shared Subscription Available、Server Keep Alive、Response Information、Server Reference、Authentication Method、Authentication Data |
| PUBLISH | Payload Format Indicator、Message Expiry Interval、Topic Alias、Response Topic、Correlation Data、User Property、Subscription Identifier、Content Type |
| PUBACK/PUBREC/PUBREL/PUBCOMP | Reason String、User Property |
| SUBSCRIBE | Subscription Identifier、User Property |
| SUBACK/UNSUBACK | Reason String、User Property |
| UNSUBSCRIBE | User Property |
| DISCONNECT | Session Expiry Interval、Reason String、User Property、Server Reference |

## 语义约定

- `User Property` 保留重复项和原始顺序。
- 同一客户端命中多个订阅时只发送一份 PUBLISH，并携带全部非空 Subscription Identifier。
- `Message Expiry Interval` 在 Broker 内使用绝对截止时间，排队、重连和 QoS 2 等待期间持续计时。
- 入站 Topic Alias 仅对来源连接有效，不进入路由消息属性。
- 客户端不得在 PUBLISH 中发送 Subscription Identifier。
- `Payload Format Indicator=UTF-8` 时验证 Payload；QoS 1/2 通过对应 ACK 返回 `Payload Format Invalid`，QoS 0 发送相同原因的 DISCONNECT。
- Request Problem Information 为 `false` 时，控制报文不发送 Reason String 和 User Property。
- Request Response Information 为 `true` 且 Broker 配置了 Response Information 时，CONNACK 返回该属性。
- 目标客户端 Maximum Packet Size 无法容纳 PUBLISH 时，仅丢弃该接收者的投递并触发 `PacketTooLarge` Observer。
- 回放与恢复会清理旧数据中不应跨连接存在的 Topic Alias、客户端 Subscription Identifier 和 Will Delay 属性。

## 当前不支持

- Enhanced Authentication 和完整 AUTH 交换。携带 Authentication Method 的 CONNECT 会收到 `Bad Authentication Method`。
- 自动出站 Topic Alias 分配。Broker 可解析和编码该属性，但不会主动压缩出站 Topic。
- AUTH 报文流程、Packet Inspector 和报文修改钩子。
