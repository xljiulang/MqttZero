namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化消息的类别。
/// </summary>
public enum MqttZeroPersistedMessageKind
{
    /// <summary>Queued for future delivery.</summary>
    Queued,
    /// <summary>Outbound QoS 1/2 in flight.</summary>
    Outbound,
    /// <summary>Inbound QoS 2 awaiting PUBREL.</summary>
    InboundQos2,

    /// <summary>Accepted inbound QoS 1 awaiting background routing.</summary>
    InboundDispatch
}
