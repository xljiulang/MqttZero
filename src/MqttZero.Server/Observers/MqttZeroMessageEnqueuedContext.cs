using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Observers;

/// <summary>
/// 消息入队事件。
/// </summary>
public sealed class MqttZeroMessageEnqueuedContext
{
    /// <summary>Gets the connected sender.</summary>
    public MqttZeroConnectedClient? Sender { get; init; }
    /// <summary>Gets the receiver information.</summary>
    public required MqttZeroClientConnectionInfo ReceiverInfo { get; init; }
    /// <summary>Gets the connected receiver when online.</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>Gets the topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>Gets the payload length.</summary>
    public long PayloadLength { get; init; }
    /// <summary>Gets the QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
}
