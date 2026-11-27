namespace MqttZero.Server.Sessions;

/// <summary>
/// 会话的只读快照。
/// </summary>
public sealed class MqttZeroSessionSnapshot
{
    /// <summary>
    /// Gets the client identifier.
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>
    /// Gets a value indicating whether a network connection is attached.
    /// </summary>
    public bool IsConnected { get; init; }

    /// <summary>
    /// Gets the latest Clean Start flag.
    /// </summary>
    public bool CleanStart { get; init; }

    /// <summary>
    /// Gets a value indicating whether the session can be restored.
    /// </summary>
    public bool SessionPresent { get; init; }

    /// <summary>
    /// Gets the configured session expiry interval in seconds.
    /// </summary>
    public uint ExpiryIntervalSeconds { get; init; }

    /// <summary>
    /// Gets the next session expiry time when disconnected.
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>
    /// Gets the subscription count.
    /// </summary>
    public int SubscriptionCount { get; init; }

    /// <summary>
    /// Gets the pending message count.
    /// </summary>
    public int PendingMessageCount { get; init; }

    /// <summary>
    /// Gets the in-flight QoS 1 and QoS 2 message count.
    /// </summary>
    public int InflightMessageCount { get; init; }

    /// <summary>
    /// Gets the inbound QoS 2 message count awaiting PUBREL.
    /// </summary>
    public int InboundQos2MessageCount { get; init; }

    /// <summary>
    /// Gets a value indicating whether the session stores a will message.
    /// </summary>
    public bool HasWill { get; init; }

    /// <summary>
    /// Gets the will topic when a will message is stored.
    /// </summary>
    public string? WillTopic { get; init; }

    /// <summary>
    /// Gets the time at which this snapshot was sampled.
    /// </summary>
    public DateTimeOffset SampledAt { get; init; }
}
