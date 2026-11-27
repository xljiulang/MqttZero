using MqttZero.Protocol;

namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的 Broker 消息。
/// </summary>
public sealed class MqttZeroPersistedMessage
{
    /// <summary>获取稳定的消息标识。</summary>
    public long MessageId { get; init; }
    /// <summary>获取所属客户端标识。</summary>
    public required string ClientId { get; init; }
    /// <summary>获取消息类别。</summary>
    public MqttZeroPersistedMessageKind Kind { get; init; }
    /// <summary>获取投递状态。</summary>
    public MqttZeroPersistedDeliveryState State { get; init; }
    /// <summary>获取报文标识符。</summary>
    public ushort PacketIdentifier { get; init; }
    /// <summary>获取主题。</summary>
    public required ReadOnlyMemory<byte> Topic { get; init; }
    /// <summary>获取属性。</summary>
    public ReadOnlyMemory<byte> Properties { get; init; }
    /// <summary>获取负载。</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }
    /// <summary>获取服务质量等级。</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>获取是否设置 RETAIN 标志。</summary>
    public bool Retain { get; init; }
    /// <summary>获取该投递的全部订阅标识符。</summary>
    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; init; }
    /// <summary>获取过期时间。</summary>
    public DateTimeOffset? ExpiresAt { get; init; }
}
