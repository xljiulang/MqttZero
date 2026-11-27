using MqttZero.Server.Clients;
using System.Buffers;

namespace MqttZero.Server.Observers;

/// <summary>
/// 队列消息被覆盖事件。
/// </summary>
public sealed class MqttZeroQueueMessageOverwrittenContext
{
    /// <summary>获取接收者信息。</summary>
    public required MqttZeroClientConnectionInfo ReceiverInfo { get; init; }
    /// <summary>获取在线的接收者。</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>获取被覆盖的主题。</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
}
