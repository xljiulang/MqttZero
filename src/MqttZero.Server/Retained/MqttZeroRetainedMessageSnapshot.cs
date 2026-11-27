using MqttZero.Protocol;
namespace MqttZero.Server.Retained;

/// <summary>
/// 保留消息的元数据快照。
/// </summary>
public sealed class MqttZeroRetainedMessageSnapshot
{
    /// <summary>
    /// 获取保留消息的主题。
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// 获取负载字节数。
    /// </summary>
    public int PayloadLength { get; init; }

    /// <summary>
    /// 获取保留消息的服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
}
