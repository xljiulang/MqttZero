using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Observers;

/// <summary>
/// 消息入队事件。
/// </summary>
public sealed class MqttZeroMessageEnqueuedContext
{
    /// <summary>获取已连接的发送者。</summary>
    public MqttZeroConnectedClient? Sender { get; init; }
    /// <summary>获取接收者信息。</summary>
    public required MqttZeroClientConnectionInfo ReceiverInfo { get; init; }
    /// <summary>获取在线的接收者。</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>获取主题。</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>获取负载长度。</summary>
    public long PayloadLength { get; init; }
    /// <summary>获取服务质量等级。</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
}
