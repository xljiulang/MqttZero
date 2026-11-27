namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的投递进度。
/// </summary>
public enum MqttZeroPersistedDeliveryState
{
    /// <summary>Queued for delivery.</summary>
    Queued,
    /// <summary>Awaiting PUBACK.</summary>
    AwaitPubAck,
    /// <summary>Awaiting PUBREC.</summary>
    AwaitPubRec,
    /// <summary>Awaiting PUBCOMP.</summary>
    AwaitPubComp
}
