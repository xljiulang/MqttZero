using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// 一条主题过滤器订阅请求。
/// </summary>
public sealed class MqttSubscriptionRequest
{
    /// <summary>
    /// 获取或设置 UTF-8 主题过滤器。
    /// </summary>
    public ReadOnlySequence<byte> TopicFilter { get; set; }

    /// <summary>
    /// 获取或设置请求的服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// 获取或设置是否抑制该客户端自身的发布。
    /// </summary>
    public bool NoLocal { get; set; }

    /// <summary>
    /// 获取或设置是否保留 RETAIN 标志。
    /// </summary>
    public bool RetainAsPublished { get; set; }

    /// <summary>
    /// 获取或设置保留消息的处理方式。
    /// </summary>
    public MqttRetainHandling RetainHandling { get; set; }
}
