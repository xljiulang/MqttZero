using MqttZero.Server.Clients;
using System.Buffers;

namespace MqttZero.Server.Observers;

/// <summary>
/// 消息被丢弃事件。
/// </summary>
public sealed class MqttZeroMessageDroppedContext
{
    /// <summary>Gets the connected sender.</summary>
    public MqttZeroConnectedClient? Sender { get; init; }
    /// <summary>Gets the receiver information.</summary>
    public MqttZeroClientConnectionInfo? ReceiverInfo { get; init; }
    /// <summary>Gets the connected receiver when online.</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>Gets the topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>Gets the drop reason.</summary>
    public MqttZeroMessageDropReason Reason { get; init; }
}
