using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>
/// 拦截器可见的遗嘱请求快照。
/// </summary>
public sealed class MqttZeroWillRequest
{
    /// <summary>
    /// 获取遗嘱主题。
    /// </summary>
    public required ReadOnlySequence<byte> Topic { get; init; }

    /// <summary>
    /// 获取遗嘱负载。
    /// </summary>
    public required ReadOnlySequence<byte> Payload { get; init; }

    /// <summary>
    /// 获取遗嘱的服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }

    /// <summary>
    /// 获取遗嘱是否作为保留消息发布。
    /// </summary>
    public bool Retain { get; init; }

    /// <summary>
    /// 获取遗嘱延迟发布的秒数。
    /// </summary>
    public uint DelayInterval { get; init; }

    /// <summary>
    /// 获取遗嘱携带的 MQTT 5 属性。
    /// </summary>
    public MqttPublishProperties Properties { get; init; } = MqttPublishProperties.Empty;
}
