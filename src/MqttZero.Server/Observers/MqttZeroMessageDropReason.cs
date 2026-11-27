namespace MqttZero.Server.Observers;

/// <summary>
/// 消息被丢弃的原因。
/// </summary>
public enum MqttZeroMessageDropReason
{
    /// <summary>No subscription matched.</summary>
    NoMatchingSubscribers,
    /// <summary>The message expired.</summary>
    Expired,
    /// <summary>Offline QoS 0 storage is disabled.</summary>
    OfflineClient,
    /// <summary>An interceptor rejected the delivery.</summary>
    InterceptorRejected,
    /// <summary>The PUBLISH exceeds the receiver maximum packet size.</summary>
    PacketTooLarge
}
