using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRouteEpochManager
{
    private MqttZeroRouteEpoch _current = MqttZeroRouteEpoch.Empty;

    public MqttZeroRouteEpoch Current
    {
        get
        {
            return Volatile.Read(ref _current);
        }
    }

    public MqttZeroRouteEpoch BuildAdd(
        IReadOnlyList<MqttZeroSubscription> subscriptions,
        MqttZeroSessionState session)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(session);
        MqttZeroRouteEpoch epoch = Current;
        for (int index = 0; index < subscriptions.Count; index++)
        {
            epoch = epoch.Add(session, subscriptions[index]);
        }

        return epoch;
    }

    public MqttZeroRouteEpoch BuildRemove(
        IReadOnlyList<MqttZeroSubscription> subscriptions,
        MqttZeroSessionState session)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(session);
        MqttZeroRouteEpoch epoch = Current;
        for (int index = 0; index < subscriptions.Count; index++)
        {
            epoch = epoch.Remove(session, subscriptions[index]);
        }

        return epoch;
    }

    public void Publish(
        MqttZeroRouteEpoch expected,
        MqttZeroRouteEpoch replacement)
    {
        ArgumentNullException.ThrowIfNull(expected);
        ArgumentNullException.ThrowIfNull(replacement);
        if (!ReferenceEquals(
                Interlocked.CompareExchange(
                    ref _current,
                    replacement,
                    expected),
                expected))
        {
            throw new InvalidOperationException(
                "The route epoch changed before the subscription update committed.");
        }
    }

    public void Reset(MqttZeroRouteEpoch epoch)
    {
        ArgumentNullException.ThrowIfNull(epoch);
        Volatile.Write(ref _current, epoch);
    }

    public static MqttZeroRouteEpoch BuildFromSessions(
        IEnumerable<MqttZeroSessionState> sessions)
    {
        ArgumentNullException.ThrowIfNull(sessions);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty;
        foreach (MqttZeroSessionState session in sessions)
        {
            lock (session.SyncRoot)
            {
                for (int index = 0;
                    index < session.Subscriptions.Count;
                    index++)
                {
                    epoch = epoch.Add(
                        session,
                        session.Subscriptions[index]);
                }
            }
        }

        return epoch;
    }
}
