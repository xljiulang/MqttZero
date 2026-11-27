using MqttZero.Protocol;

namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化的遗嘱消息。
/// </summary>
public sealed class MqttZeroPersistedWill
{
    /// <summary>Gets the will topic.</summary>
    public required ReadOnlyMemory<byte> Topic { get; init; }
    /// <summary>Gets the will payload.</summary>
    public required ReadOnlyMemory<byte> Payload { get; init; }
    /// <summary>Gets the will properties.</summary>
    public ReadOnlyMemory<byte> Properties { get; init; }
    /// <summary>Gets the will QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets whether RETAIN is set.</summary>
    public bool Retain { get; init; }
    /// <summary>Gets the will delay in seconds.</summary>
    public uint DelayInterval { get; init; }
    /// <summary>Gets the will message expiry in seconds.</summary>
    public uint MessageExpiryInterval { get; init; }
}
