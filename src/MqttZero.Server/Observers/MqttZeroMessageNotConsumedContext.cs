using MqttZero.Server.Clients;
using System.Buffers;

namespace MqttZero.Server.Observers;

/// <summary>
/// 无匹配订阅者的发布事件。
/// </summary>
public sealed class MqttZeroMessageNotConsumedContext
{
    /// <summary>获取已连接的发送者。</summary>
    public MqttZeroConnectedClient? Sender { get; init; }
    /// <summary>获取主题。</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>获取负载长度。</summary>
    public long PayloadLength { get; init; }
}
