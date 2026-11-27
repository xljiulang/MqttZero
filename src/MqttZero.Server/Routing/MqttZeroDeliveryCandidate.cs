using MqttZero.Protocol;
using MqttZero.Server.Sessions;
namespace MqttZero.Server.Routing;

/// <summary>
/// 匹配到的一个应接收消息的订阅。
/// </summary>
internal readonly struct MqttZeroDeliveryCandidate
{
    /// <summary>
    /// 初始化 MqttZeroDeliveryCandidate 的新实例。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    public MqttZeroDeliveryCandidate(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        ReadOnlyMemory<uint> subscriptionIdentifiers)
    {
        Session = session;
        Subscription = subscription;
        SubscriptionIdentifiers = subscriptionIdentifiers;
    }

    /// <summary>
    /// 会话。
    /// </summary>
    public MqttZeroSessionState Session { get; }

    /// <summary>
    /// 订阅。
    /// </summary>
    public MqttZeroSubscription Subscription { get; }

    /// <summary>
    /// 订阅标识符。
    /// </summary>
    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel
    {
        get
        {
            return Subscription.QualityOfServiceLevel;
        }
    }
}
