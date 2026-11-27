using MqttZero.Protocol;
namespace MqttZero.Server.Retained;

/// <summary>
/// 保留消息的元数据快照。
/// </summary>
public sealed class MqttZeroRetainedMessageSnapshot
{
    /// <summary>
    /// Gets the retained topic.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the payload length in bytes.
    /// </summary>
    public int PayloadLength { get; init; }

    /// <summary>
    /// Gets the retained message quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
}
