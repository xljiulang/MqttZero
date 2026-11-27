using MqttZero.Protocol;

namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的订阅。
/// </summary>
public sealed class MqttZeroPersistedSubscription
{
    /// <summary>获取 UTF-8 主题过滤器。</summary>
    public required ReadOnlyMemory<byte> TopicFilter { get; init; }
    /// <summary>获取授予的服务质量等级。</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>获取是否抑制本客户端自身的消息。</summary>
    public bool NoLocal { get; init; }
    /// <summary>获取是否保留 RETAIN 标志。</summary>
    public bool RetainAsPublished { get; init; }
    /// <summary>获取订阅标识符。</summary>
    public uint? SubscriptionIdentifier { get; init; }
}
