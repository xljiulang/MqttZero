namespace MqttZero.Protocol;

/// <summary>
/// 确认类报文使用的原因码。
/// </summary>
public enum MqttReasonCode : byte
{
    /// <summary>操作成功。</summary>
    Success = 0x00,

    /// <summary>客户端正常断开时随连接发布遗嘱。</summary>
    DisconnectWithWill = 0x04,

    /// <summary>没有订阅匹配该发布。</summary>
    NoMatchingSubscribers = 0x10,

    /// <summary>服务端发生意外故障。</summary>
    UnspecifiedError = 0x80,

    /// <summary>服务端遇到实现错误。</summary>
    ImplementationSpecificError = 0x83,

    /// <summary>服务端不可用。</summary>
    ServerUnavailable = 0x88,

    /// <summary>客户端已被封禁。</summary>
    Banned = 0x8A,

    /// <summary>客户端应改用其他服务端。</summary>
    UseAnotherServer = 0x9C,

    /// <summary>服务端已迁移。</summary>
    ServerMoved = 0x9D,

    /// <summary>连接速率超限。</summary>
    ConnectionRateExceeded = 0x9F,

    /// <summary>用户名或密码无效。</summary>
    BadUserNameOrPassword = 0x86,

    /// <summary>需要额外认证。</summary>
    ContinueAuthentication = 0x18,

    /// <summary>客户端请求重新认证。</summary>
    Reauthenticate = 0x19,

    /// <summary>报文格式错误。</summary>
    MalformedPacket = 0x81,

    /// <summary>客户端违反协议规则。</summary>
    ProtocolError = 0x82,

    /// <summary>不支持请求的协议版本。</summary>
    UnsupportedProtocolVersion = 0x84,

    /// <summary>客户端标识无效。</summary>
    ClientIdentifierNotValid = 0x85,

    /// <summary>服务端忙。</summary>
    ServerBusy = 0x89,

    /// <summary>不支持该认证方法。</summary>
    BadAuthenticationMethod = 0x8C,

    /// <summary>订阅以 QoS 1 授予。</summary>
    GrantedQos1 = 0x01,

    /// <summary>订阅以 QoS 2 授予。</summary>
    GrantedQos2 = 0x02,

    /// <summary>不支持请求的服务质量等级。</summary>
    QosNotSupported = 0x9B,

    /// <summary>超出配额。</summary>
    QuotaExceeded = 0x97,

    /// <summary>操作未获授权。</summary>
    NotAuthorized = 0x87,

    /// <summary>不存在匹配的订阅。</summary>
    NoSubscriptionExisted = 0x11,

    /// <summary>主题过滤器无效。</summary>
    TopicFilterInvalid = 0x8F,

    /// <summary>主题名无效。</summary>
    TopicNameInvalid = 0x90,

    /// <summary>报文标识符已被占用。</summary>
    PacketIdentifierInUse = 0x91,

    /// <summary>未找到该报文标识符。</summary>
    PacketIdentifierNotFound = 0x92,

    /// <summary>超出接收上限。</summary>
    ReceiveMaximumExceeded = 0x93,

    /// <summary>主题别名无效。</summary>
    TopicAliasInvalid = 0x94,

    /// <summary>报文超过协商的最大尺寸。</summary>
    PacketTooLarge = 0x95,

    /// <summary>消息速率过高。</summary>
    MessageRateTooHigh = 0x96,

    /// <summary>服务端正在关闭。</summary>
    ServerShuttingDown = 0x8B,

    /// <summary>客户端会话已被另一条连接接管。</summary>
    SessionTakenOver = 0x8E,

    /// <summary>保活超时。</summary>
    KeepAliveTimeout = 0x8D,

    /// <summary>负载格式无效。</summary>
    PayloadFormatInvalid = 0x99,

    /// <summary>不支持保留消息。</summary>
    RetainNotSupported = 0x9A,

    /// <summary>不支持共享订阅。</summary>
    SharedSubscriptionsNotSupported = 0x9E,

    /// <summary>管理员操作导致连接被断开。</summary>
    AdministrativeAction = 0x98
}
