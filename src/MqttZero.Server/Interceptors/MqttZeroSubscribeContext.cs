using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// 订阅拦截器的上下文。
/// </summary>
public sealed class MqttZeroSubscribeContext
{
    /// <summary>Gets the connected client.</summary>
    public required MqttZeroConnectedClient ConnectedClient { get; init; }
    /// <summary>Gets the topic filter.</summary>
    public required ReadOnlySequence<byte> TopicFilter { get; init; }
    /// <summary>Gets the requested QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets whether local publications are suppressed.</summary>
    public bool NoLocal { get; init; }
    /// <summary>Gets whether RETAIN is preserved.</summary>
    public bool RetainAsPublished { get; init; }
    /// <summary>Gets the retained-message handling mode.</summary>
    public MqttRetainHandling RetainHandling { get; init; }
    /// <summary>Gets or sets the denial reason code.</summary>
    public MqttReasonCode ReasonCode { get; set; } = MqttReasonCode.NotAuthorized;
    /// <summary>Gets or sets an optional reason string.</summary>
    public string? ReasonString { get; set; }
}
