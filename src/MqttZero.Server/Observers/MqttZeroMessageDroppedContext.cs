using MqttZero.Server.Clients;
using System.Buffers;

namespace MqttZero.Server.Observers;

/// <summary>
/// 消息被丢弃事件。
/// </summary>
public sealed class MqttZeroMessageDroppedContext
{
    /// <summary>获取已连接的发送者。</summary>
    public MqttZeroConnectedClient? Sender { get; init; }
    /// <summary>获取接收者信息。</summary>
    public MqttZeroClientConnectionInfo? ReceiverInfo { get; init; }
    /// <summary>获取在线的接收者。</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>获取主题。</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>获取丢弃原因。</summary>
    public MqttZeroMessageDropReason Reason { get; init; }
}
