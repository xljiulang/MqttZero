using MqttZero.Server.Clients;
using System.Buffers;

namespace MqttZero.Server.Observers;

/// <summary>
/// 队列消息被覆盖事件。
/// </summary>
public sealed class MqttZeroQueueMessageOverwrittenContext
{
    /// <summary>Gets the receiver information.</summary>
    public required MqttZeroClientConnectionInfo ReceiverInfo { get; init; }
    /// <summary>Gets the connected receiver when online.</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>Gets the overwritten topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
}
