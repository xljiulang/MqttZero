using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// 单个接收者投递的授权数据。
/// </summary>
public sealed class MqttZeroEnqueueContext
{
    /// <summary>Gets the connected sender.</summary>
    public MqttZeroConnectedClient? Sender { get; init; }
    /// <summary>Gets the receiver information.</summary>
    public required MqttZeroClientConnectionInfo ReceiverInfo { get; init; }
    /// <summary>Gets the connected receiver when online.</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>Gets the UTF-8 topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>Gets the payload length.</summary>
    public long PayloadLength { get; init; }
    /// <summary>Gets the delivery QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets whether RETAIN is set.</summary>
    public bool Retain { get; init; }
    /// <summary>Gets all subscription identifiers attached to the delivery.</summary>
    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; init; }
}
