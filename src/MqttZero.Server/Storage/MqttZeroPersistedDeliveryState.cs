namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的投递进度。
/// </summary>
public enum MqttZeroPersistedDeliveryState
{
    /// <summary>已入队等待投递。</summary>
    Queued,
    /// <summary>等待 PUBACK。</summary>
    AwaitPubAck,
    /// <summary>等待 PUBREC。</summary>
    AwaitPubRec,
    /// <summary>等待 PUBCOMP。</summary>
    AwaitPubComp
}
