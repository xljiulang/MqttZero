using MqttZero.Protocol;

namespace MqttZero.Server.Broker;

/// <summary>
/// Describes the outcome of evaluating one UNSUBSCRIBE entry.
/// </summary>
internal readonly struct MqttZeroUnsubscriptionDecision
{
    private MqttZeroUnsubscriptionDecision(
        MqttReasonCode reasonCode,
        bool changed,
        bool closeConnection)
    {
        ReasonCode = reasonCode;
        Changed = changed;
        CloseConnection = closeConnection;
    }

    public MqttReasonCode ReasonCode { get; }

    public bool Changed { get; }

    public bool CloseConnection { get; }

    public static MqttZeroUnsubscriptionDecision Unsubscribed(bool changed)
    {
        return new MqttZeroUnsubscriptionDecision(
            changed
                ? MqttReasonCode.Success
                : MqttReasonCode.NoSubscriptionExisted,
            changed,
            false);
    }

    public static MqttZeroUnsubscriptionDecision Consumed(MqttReasonCode reasonCode)
    {
        return new MqttZeroUnsubscriptionDecision(reasonCode, false, false);
    }

    public static MqttZeroUnsubscriptionDecision Closed(MqttReasonCode reasonCode)
    {
        return new MqttZeroUnsubscriptionDecision(reasonCode, false, true);
    }
}
