using MqttZero.Protocol;

namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的保留消息。
/// </summary>
public sealed class MqttZeroPersistedRetainedMessage
{
    /// <summary>获取主题。</summary>
    public required ReadOnlyMemory<byte> Topic { get; init; }
    /// <summary>获取属性。</summary>
    public ReadOnlyMemory<byte> Properties { get; init; }
    /// <summary>获取负载。</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }
    /// <summary>获取服务质量等级。</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>获取过期时间。</summary>
    public DateTimeOffset? ExpiresAt { get; init; }
}
