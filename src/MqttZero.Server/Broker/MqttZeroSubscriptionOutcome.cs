namespace MqttZero.Server.Broker;

/// <summary>
/// 单条订阅的处理方式。
/// </summary>
internal enum MqttZeroSubscriptionOutcome
{
    Subscribed,
    Consumed,
    QuotaExceeded,
    CloseConnection
}
