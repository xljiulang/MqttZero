using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRouteEpochManager
{
    private readonly MqttZeroSharedGroupCursors _cursors;
    private MqttZeroRouteEpoch _current;

    public MqttZeroRouteEpochManager()
        : this(new MqttZeroSharedGroupCursors())
    {
    }

    public MqttZeroRouteEpochManager(MqttZeroSharedGroupCursors cursors)
    {
        _cursors = cursors ?? throw new ArgumentNullException(nameof(cursors));
        _current = new MqttZeroRouteEpoch(
            0,
            MqttZeroRouteTrieNode.Empty,
            cursors);
    }

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

    public void ResetToEmpty()
    {
        Volatile.Write(
            ref _current,
            new MqttZeroRouteEpoch(
                0,
                MqttZeroRouteTrieNode.Empty,
                _cursors));
    }

    public static MqttZeroRouteEpoch BuildFromSessions(
        IEnumerable<MqttZeroSessionState> sessions)
    {
        return BuildFromSessions(sessions, new MqttZeroSharedGroupCursors());
    }

    public static MqttZeroRouteEpoch BuildFromSessions(
        IEnumerable<MqttZeroSessionState> sessions,
        MqttZeroSharedGroupCursors cursors)
    {
        ArgumentNullException.ThrowIfNull(sessions);
        ArgumentNullException.ThrowIfNull(cursors);
        MqttZeroRouteEpoch epoch = new(
            0,
            MqttZeroRouteTrieNode.Empty,
            cursors);
        foreach (MqttZeroSessionState session in sessions)
        {
            MqttZeroSubscription[] subscriptions =
                session.Subscriptions.Snapshot();
            for (int index = 0;
                index < subscriptions.Length;
                index++)
            {
                epoch = epoch.Add(
                    session,
                    subscriptions[index]);
            }
        }

        return epoch;
    }
}
