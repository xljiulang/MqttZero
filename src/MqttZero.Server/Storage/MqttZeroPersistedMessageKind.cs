namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化消息的类别。
/// </summary>
public enum MqttZeroPersistedMessageKind
{
    /// <summary>已入队等待后续投递。</summary>
    Queued,
    /// <summary>出站 QoS 1/2 在途。</summary>
    Outbound,
    /// <summary>入站 QoS 2 等待 PUBREL。</summary>
    InboundQos2,

    /// <summary>已受理的入站 QoS 1，等待后台路由。</summary>
    InboundDispatch
}
