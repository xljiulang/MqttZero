using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// 单个接收者投递的授权数据。
/// </summary>
public sealed class MqttZeroEnqueueContext
{
    /// <summary>获取已连接的发送者。</summary>
    public MqttZeroConnectedClient? Sender { get; init; }
    /// <summary>获取接收者信息。</summary>
    public required MqttZeroClientConnectionInfo ReceiverInfo { get; init; }
    /// <summary>获取在线的接收者。</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>获取 UTF-8 主题。</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>获取负载长度。</summary>
    public long PayloadLength { get; init; }
    /// <summary>获取投递的服务质量等级。</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>获取是否设置 RETAIN 标志。</summary>
    public bool Retain { get; init; }
    /// <summary>获取挂在该投递上的全部订阅标识符。</summary>
    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; init; }
}
