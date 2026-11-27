using MqttZero.Protocol;

namespace MqttZero.Server.Clients;

/// <summary>
/// 服务端发起退订的结果。
/// </summary>
public sealed class MqttZeroUnsubscribeResult
{
    /// <summary>Gets the topic filter.</summary>
    public required string TopicFilter { get; init; }
    /// <summary>Gets the result reason code.</summary>
    public MqttReasonCode ReasonCode { get; init; }
}
