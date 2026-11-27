using MqttZero.Protocol;

namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的保留消息。
/// </summary>
public sealed class MqttZeroPersistedRetainedMessage
{
    /// <summary>Gets the topic.</summary>
    public required ReadOnlyMemory<byte> Topic { get; init; }
    /// <summary>Gets the properties.</summary>
    public ReadOnlyMemory<byte> Properties { get; init; }
    /// <summary>Gets the payload.</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }
    /// <summary>Gets the QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets the expiration time.</summary>
    public DateTimeOffset? ExpiresAt { get; init; }
}
