using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// 退订拦截器的上下文。
/// </summary>
public sealed class MqttZeroUnsubscribeContext
{
    /// <summary>获取已连接客户端。</summary>
    public required MqttZeroConnectedClient ConnectedClient { get; init; }
    /// <summary>获取主题过滤器。</summary>
    public required ReadOnlySequence<byte> TopicFilter { get; init; }
    /// <summary>获取或设置拒绝原因码。</summary>
    public MqttReasonCode ReasonCode { get; set; } = MqttReasonCode.NotAuthorized;
    /// <summary>获取或设置可选的原因字符串。</summary>
    public string? ReasonString { get; set; }
}
