using MqttZero.Protocol;

namespace MqttZero.Server.Clients;

/// <summary>
/// 服务端发起退订的结果。
/// </summary>
public sealed class MqttZeroUnsubscribeResult
{
    /// <summary>获取主题过滤器。</summary>
    public required string TopicFilter { get; init; }
    /// <summary>获取结果原因码。</summary>
    public MqttReasonCode ReasonCode { get; init; }
}
