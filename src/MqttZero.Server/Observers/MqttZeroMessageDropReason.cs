namespace MqttZero.Server.Observers;

/// <summary>
/// 消息被丢弃的原因。
/// </summary>
public enum MqttZeroMessageDropReason
{
    /// <summary>没有订阅匹配。</summary>
    NoMatchingSubscribers,
    /// <summary>消息已过期。</summary>
    Expired,
    /// <summary>离线 QoS 0 存储已关闭。</summary>
    OfflineClient,
    /// <summary>拦截器拒绝了该投递。</summary>
    InterceptorRejected,
    /// <summary>PUBLISH 超过接收者的最大报文大小。</summary>
    PacketTooLarge
}
