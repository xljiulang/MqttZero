using MqttZero.Protocol;

namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的遗嘱消息。
/// </summary>
public sealed class MqttZeroPersistedWill
{
    /// <summary>获取遗嘱主题。</summary>
    public required ReadOnlyMemory<byte> Topic { get; init; }
    /// <summary>获取遗嘱负载。</summary>
    public required ReadOnlyMemory<byte> Payload { get; init; }
    /// <summary>获取遗嘱属性。</summary>
    public ReadOnlyMemory<byte> Properties { get; init; }
    /// <summary>获取遗嘱的服务质量等级。</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>获取是否设置 RETAIN 标志。</summary>
    public bool Retain { get; init; }
    /// <summary>获取遗嘱延迟秒数。</summary>
    public uint DelayInterval { get; init; }
    /// <summary>获取遗嘱消息的过期秒数。</summary>
    public uint MessageExpiryInterval { get; init; }
}
