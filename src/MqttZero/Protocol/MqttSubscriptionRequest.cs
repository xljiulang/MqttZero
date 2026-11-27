using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// 一条主题过滤器订阅请求。
/// </summary>
public sealed class MqttSubscriptionRequest
{
    /// <summary>
    /// Gets or sets the UTF-8 topic filter.
    /// </summary>
    public ReadOnlySequence<byte> TopicFilter { get; set; }

    /// <summary>
    /// Gets or sets the requested quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether publications from this client are suppressed.
    /// </summary>
    public bool NoLocal { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the RETAIN flag is preserved.
    /// </summary>
    public bool RetainAsPublished { get; set; }

    /// <summary>
    /// Gets or sets the retained-message handling mode.
    /// </summary>
    public MqttRetainHandling RetainHandling { get; set; }
}
