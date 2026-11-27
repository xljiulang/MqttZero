using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// PUBLISH 与 Will 携带的 MQTT 5 属性集合。
/// </summary>
public sealed class MqttPublishProperties
{
    /// <summary>
    /// 获取共享的空属性实例。
    /// </summary>
    public static MqttPublishProperties Empty { get; } = new();

    /// <summary>
    /// 获取负载格式指示。
    /// </summary>
    public MqttPayloadFormatIndicator? PayloadFormatIndicator { get; init; }

    /// <summary>
    /// 获取剩余的消息过期秒数。
    /// </summary>
    public uint? MessageExpiryInterval { get; init; }

    /// <summary>
    /// 获取内容类型。
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// 获取响应主题。
    /// </summary>
    public string? ResponseTopic { get; init; }

    /// <summary>
    /// 获取不透明的关联数据。
    /// </summary>
    public ReadOnlyMemory<byte>? CorrelationData { get; init; }

    /// <summary>
    /// 获取有序的用户属性；重复名称会被保留。
    /// </summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } = Array.Empty<MqttUserProperty>();
}
