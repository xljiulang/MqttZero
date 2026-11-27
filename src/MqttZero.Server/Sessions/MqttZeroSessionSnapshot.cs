namespace MqttZero.Server.Sessions;

/// <summary>
/// 会话的只读快照。
/// </summary>
public sealed class MqttZeroSessionSnapshot
{
    /// <summary>
    /// 获取客户端标识。
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>
    /// 获取是否已挂接网络连接。
    /// </summary>
    public bool IsConnected { get; init; }

    /// <summary>
    /// 获取最近一次的清理会话标志。
    /// </summary>
    public bool CleanStart { get; init; }

    /// <summary>
    /// 获取该会话是否可恢复。
    /// </summary>
    public bool SessionPresent { get; init; }

    /// <summary>
    /// 获取配置的会话过期秒数。
    /// </summary>
    public uint ExpiryIntervalSeconds { get; init; }

    /// <summary>
    /// 获取断开状态下的下一次会话过期时间。
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>
    /// 获取订阅数量。
    /// </summary>
    public int SubscriptionCount { get; init; }

    /// <summary>
    /// 获取待发送消息数量。
    /// </summary>
    public int PendingMessageCount { get; init; }

    /// <summary>
    /// 获取在途的 QoS 1 与 QoS 2 消息数量。
    /// </summary>
    public int InflightMessageCount { get; init; }

    /// <summary>
    /// 获取等待 PUBREL 的入站 QoS 2 消息数量。
    /// </summary>
    public int InboundQos2MessageCount { get; init; }

    /// <summary>
    /// 获取该会话是否保存了遗嘱消息。
    /// </summary>
    public bool HasWill { get; init; }

    /// <summary>
    /// 获取遗嘱主题；仅在保存了遗嘱消息时有值。
    /// </summary>
    public string? WillTopic { get; init; }

    /// <summary>
    /// 获取该快照的采样时间。
    /// </summary>
    public DateTimeOffset SampledAt { get; init; }
}
