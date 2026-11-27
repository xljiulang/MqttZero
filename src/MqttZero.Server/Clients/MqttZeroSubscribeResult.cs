using MqttZero.Protocol;

namespace MqttZero.Server.Clients;

/// <summary>
/// 服务端发起订阅的结果。
/// </summary>
public sealed class MqttZeroSubscribeResult
{
    /// <summary>获取主题过滤器。</summary>
    public required string TopicFilter { get; init; }
    /// <summary>获取授予的服务质量等级。</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>获取结果原因码。</summary>
    public MqttReasonCode ReasonCode { get; init; }
}
