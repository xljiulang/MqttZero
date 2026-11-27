using MqttZero.Protocol;
using MqttZero.Server.Sessions;
namespace MqttZero.Server.Routing;

/// <summary>
/// Describes one matched subscription that should receive a published message.
/// </summary>
/// <remarks>
/// Consumed by the test-only <see cref="MqttZeroRouteEpochMatch"/> path. Production
/// routing writes batches through <see cref="MqttZeroRoutePlanBuffer"/>.
/// </remarks>
internal readonly struct MqttZeroDeliveryCandidate
{
    public MqttZeroDeliveryCandidate(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        ReadOnlyMemory<uint> subscriptionIdentifiers)
    {
        Session = session;
        Subscription = subscription;
        SubscriptionIdentifiers = subscriptionIdentifiers;
    }

    public MqttZeroSessionState Session { get; }

    public MqttZeroSubscription Subscription { get; }

    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel
    {
        get
        {
            return Subscription.QualityOfServiceLevel;
        }
    }
}
