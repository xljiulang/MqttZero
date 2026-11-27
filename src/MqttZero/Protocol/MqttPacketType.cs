namespace MqttZero.Protocol;

/// <summary>
/// MQTT 控制报文类型。
/// </summary>
internal enum MqttPacketType : byte
{
    /// <summary>客户端连接请求。</summary>
    Connect = 1,

    /// <summary>连接确认。</summary>
    ConnAck = 2,

    /// <summary>应用消息发布。</summary>
    Publish = 3,

    /// <summary>QoS 1 发布确认。</summary>
    PubAck = 4,

    /// <summary>QoS 2 发布已收到。</summary>
    PubRec = 5,

    /// <summary>QoS 2 发布释放。</summary>
    PubRel = 6,

    /// <summary>QoS 2 发布完成。</summary>
    PubComp = 7,

    /// <summary>主题订阅请求。</summary>
    Subscribe = 8,

    /// <summary>主题订阅确认。</summary>
    SubAck = 9,

    /// <summary>主题退订请求。</summary>
    Unsubscribe = 10,

    /// <summary>主题退订确认。</summary>
    UnsubAck = 11,

    /// <summary>心跳请求。</summary>
    PingReq = 12,

    /// <summary>心跳响应。</summary>
    PingResp = 13,

    /// <summary>断开连接通知。</summary>
    Disconnect = 14
}
