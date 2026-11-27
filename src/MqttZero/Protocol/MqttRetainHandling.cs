namespace MqttZero.Protocol;

/// <summary>
/// 订阅时保留消息的投递方式。
/// </summary>
public enum MqttRetainHandling : byte
{
    /// <summary>Send retained messages when the subscription is created.</summary>
    SendAtSubscribe = 0,

    /// <summary>Send retained messages only when the subscription is new.</summary>
    SendAtSubscribeIfNewSubscriptionOnly = 1,

    /// <summary>Do not send retained messages when subscribing.</summary>
    DoNotSendOnSubscribe = 2
}
