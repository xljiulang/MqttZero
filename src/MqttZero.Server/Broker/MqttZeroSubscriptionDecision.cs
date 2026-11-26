using MqttZero.Protocol;

namespace MqttZero.Server.Broker;

/// <summary>
/// Describes the outcome of evaluating one SUBSCRIBE entry.
/// </summary>
internal readonly struct MqttZeroSubscriptionDecision
{
    private MqttZeroSubscriptionDecision(
        MqttZeroSubscriptionOutcome outcome,
        MqttReasonCode reasonCode,
        bool existed)
    {
        Outcome = outcome;
        ReasonCode = reasonCode;
        Existed = existed;
    }

    public MqttZeroSubscriptionOutcome Outcome { get; }

    public MqttReasonCode ReasonCode { get; }

    public bool Existed { get; }

    public bool CloseConnection
    {
        get
        {
            return Outcome == MqttZeroSubscriptionOutcome.CloseConnection;
        }
    }

    public static MqttZeroSubscriptionDecision Subscribed(
        bool existed,
        MqttReasonCode reasonCode)
    {
        return new MqttZeroSubscriptionDecision(
            MqttZeroSubscriptionOutcome.Subscribed,
            reasonCode,
            existed);
    }

    public static MqttZeroSubscriptionDecision Consumed(MqttReasonCode reasonCode)
    {
        return new MqttZeroSubscriptionDecision(
            MqttZeroSubscriptionOutcome.Consumed,
            reasonCode,
            false);
    }

    public static MqttZeroSubscriptionDecision QuotaExceeded()
    {
        return new MqttZeroSubscriptionDecision(
            MqttZeroSubscriptionOutcome.QuotaExceeded,
            MqttReasonCode.QuotaExceeded,
            false);
    }

    public static MqttZeroSubscriptionDecision Closed(MqttReasonCode reasonCode)
    {
        return new MqttZeroSubscriptionDecision(
            MqttZeroSubscriptionOutcome.CloseConnection,
            reasonCode,
            false);
    }
}
