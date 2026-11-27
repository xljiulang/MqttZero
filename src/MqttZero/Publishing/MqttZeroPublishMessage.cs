using System.Buffers;
using MqttZero.Protocol;

namespace MqttZero.Publishing;

/// <summary>
/// 服务端发起的一条 PUBLISH 消息描述。
/// </summary>
public sealed class MqttZeroPublishMessage
{
    /// <summary>
    /// 获取 MQTT 主题名。
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// 获取负载序列。
    /// </summary>
    public required ReadOnlySequence<byte> Payload { get; init; }

    /// <summary>
    /// 获取请求的服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; } = MqttQualityOfServiceLevel.AtMostOnce;

    /// <summary>
    /// 获取消息是否应作为保留消息发布。
    /// </summary>
    public bool Retain { get; init; }

    /// <summary>
    /// 获取随消息传递的 MQTT 5 属性。
    /// </summary>
    public MqttPublishProperties Properties { get; init; } = MqttPublishProperties.Empty;
}
