namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的会话元数据。
/// </summary>
public sealed class MqttZeroPersistedSession
{
    /// <summary>获取客户端标识。</summary>
    public required string ClientId { get; init; }
    /// <summary>获取清理会话标志。</summary>
    public bool CleanStart { get; init; }
    /// <summary>获取过期时间（秒）。</summary>
    public uint ExpiryInterval { get; init; }
    /// <summary>获取断开时间戳。</summary>
    public DateTimeOffset? DisconnectedAt { get; init; }
    /// <summary>获取遗嘱消息。</summary>
    public MqttZeroPersistedWill? Will { get; init; }
    /// <summary>获取订阅集合。</summary>
    public IReadOnlyList<MqttZeroPersistedSubscription> Subscriptions { get; init; } = [];
}
