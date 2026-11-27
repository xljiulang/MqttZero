using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// 订阅拦截器的上下文。
/// </summary>
public sealed class MqttZeroSubscribeContext
{
    /// <summary>获取已连接客户端。</summary>
    public required MqttZeroConnectedClient ConnectedClient { get; init; }
    /// <summary>获取主题过滤器。</summary>
    public required ReadOnlySequence<byte> TopicFilter { get; init; }
    /// <summary>获取请求的服务质量等级。</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>获取是否抑制本客户端自身的发布。</summary>
    public bool NoLocal { get; init; }
    /// <summary>获取是否保留 RETAIN 标志。</summary>
    public bool RetainAsPublished { get; init; }
    /// <summary>获取保留消息处理方式。</summary>
    public MqttRetainHandling RetainHandling { get; init; }
    /// <summary>获取或设置拒绝原因码。</summary>
    public MqttReasonCode ReasonCode { get; set; } = MqttReasonCode.NotAuthorized;
    /// <summary>获取或设置可选的原因字符串。</summary>
    public string? ReasonString { get; set; }
}
