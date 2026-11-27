using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

/// <summary>
/// 路由快照的构建、发布与回收。
/// </summary>
internal sealed class MqttZeroRouteEpochManager
{
    private readonly MqttZeroSharedGroupCursors _cursors;
    private MqttZeroRouteEpoch _current;

    /// <summary>
    /// 初始化 MqttZeroRouteEpochManager 的新实例。
    /// </summary>
    public MqttZeroRouteEpochManager()
        : this(new MqttZeroSharedGroupCursors())
    {
    }

    /// <summary>
    /// 初始化 MqttZeroRouteEpochManager 的新实例。
    /// </summary>
    /// <param name="cursors">游标。</param>
    public MqttZeroRouteEpochManager(MqttZeroSharedGroupCursors cursors)
    {
        _cursors = cursors ?? throw new ArgumentNullException(nameof(cursors));
        _current = new MqttZeroRouteEpoch(
            0,
            MqttZeroRouteTrieNode.Empty,
            cursors);
    }

    /// <summary>
    /// Current 的值。
    /// </summary>
    public MqttZeroRouteEpoch Current
    {
        get
        {
            return Volatile.Read(ref _current);
        }
    }

    /// <summary>
    /// 构建Add。
    /// </summary>
    /// <param name="subscriptions">订阅。</param>
    /// <param name="session">会话。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 构建Remove。
    /// </summary>
    /// <param name="subscriptions">订阅。</param>
    /// <param name="session">会话。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 发布。
    /// </summary>
    /// <param name="expected">对应的 expected。</param>
    /// <param name="replacement">对应的 replacement。</param>
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

    /// <summary>
    /// 重置。
    /// </summary>
    /// <param name="epoch">路由快照。</param>
    public void Reset(MqttZeroRouteEpoch epoch)
    {
        ArgumentNullException.ThrowIfNull(epoch);
        Volatile.Write(ref _current, epoch);
    }

    /// <summary>
    /// 重置To空。
    /// </summary>
    public void ResetToEmpty()
    {
        Volatile.Write(
            ref _current,
            new MqttZeroRouteEpoch(
                0,
                MqttZeroRouteTrieNode.Empty,
                _cursors));
    }

    /// <summary>
    /// 构建FromSessions。
    /// </summary>
    /// <param name="sessions">对应的 sessions。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroRouteEpoch BuildFromSessions(
        IEnumerable<MqttZeroSessionState> sessions)
    {
        return BuildFromSessions(sessions, new MqttZeroSharedGroupCursors());
    }

    /// <summary>
    /// 构建FromSessions。
    /// </summary>
    /// <param name="sessions">对应的 sessions。</param>
    /// <param name="cursors">游标。</param>
    /// <returns>对应的结果。</returns>
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
