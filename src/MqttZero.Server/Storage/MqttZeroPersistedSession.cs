namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的会话元数据。
/// </summary>
public sealed class MqttZeroPersistedSession
{
    /// <summary>Gets the client identifier.</summary>
    public required string ClientId { get; init; }
    /// <summary>Gets the Clean Start flag.</summary>
    public bool CleanStart { get; init; }
    /// <summary>Gets the expiry interval in seconds.</summary>
    public uint ExpiryInterval { get; init; }
    /// <summary>Gets the disconnected timestamp.</summary>
    public DateTimeOffset? DisconnectedAt { get; init; }
    /// <summary>Gets the will message.</summary>
    public MqttZeroPersistedWill? Will { get; init; }
    /// <summary>Gets the subscriptions.</summary>
    public IReadOnlyList<MqttZeroPersistedSubscription> Subscriptions { get; init; } = [];
}
