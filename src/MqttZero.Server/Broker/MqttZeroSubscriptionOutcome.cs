namespace MqttZero.Server.Broker;

/// <summary>
/// Identifies how one SUBSCRIBE entry was handled.
/// </summary>
internal enum MqttZeroSubscriptionOutcome
{
    Subscribed,
    Consumed,
    QuotaExceeded,
    CloseConnection
}
