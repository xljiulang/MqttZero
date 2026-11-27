namespace MqttZero.Protocol;

/// <summary>
/// 订阅时保留消息的投递方式。
/// </summary>
public enum MqttRetainHandling : byte
{
    /// <summary>订阅创建时发送保留消息。</summary>
    SendAtSubscribe = 0,

    /// <summary>仅当订阅是新建时才发送保留消息。</summary>
    SendAtSubscribeIfNewSubscriptionOnly = 1,

    /// <summary>订阅时不发送保留消息。</summary>
    DoNotSendOnSubscribe = 2
}
