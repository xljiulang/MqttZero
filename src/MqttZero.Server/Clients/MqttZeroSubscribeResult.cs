using MqttZero.Protocol;

namespace MqttZero.Server.Clients;

/// <summary>
/// 服务端发起订阅的结果。
/// </summary>
public sealed class MqttZeroSubscribeResult
{
    /// <summary>Gets the topic filter.</summary>
    public required string TopicFilter { get; init; }
    /// <summary>Gets the granted QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets the result reason code.</summary>
    public MqttReasonCode ReasonCode { get; init; }
}
