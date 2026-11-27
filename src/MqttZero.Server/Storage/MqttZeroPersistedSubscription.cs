using MqttZero.Protocol;

namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的订阅。
/// </summary>
public sealed class MqttZeroPersistedSubscription
{
    /// <summary>Gets the UTF-8 topic filter.</summary>
    public required ReadOnlyMemory<byte> TopicFilter { get; init; }
    /// <summary>Gets the granted QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets whether local messages are suppressed.</summary>
    public bool NoLocal { get; init; }
    /// <summary>Gets whether RETAIN is preserved.</summary>
    public bool RetainAsPublished { get; init; }
    /// <summary>Gets the subscription identifier.</summary>
    public uint? SubscriptionIdentifier { get; init; }
}
