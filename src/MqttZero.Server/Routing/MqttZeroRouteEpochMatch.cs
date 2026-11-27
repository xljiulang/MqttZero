using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

/// <summary>
/// 一次主题匹配累积的投递候选，仅用于测试与基准。
/// </summary>
internal ref struct MqttZeroRouteEpochMatch
{
    private readonly List<MqttZeroDeliveryCandidate> _deliveries;

    /// <summary>
    /// 初始化 MqttZeroRouteEpochMatch 的新实例。
    /// </summary>
    public MqttZeroRouteEpochMatch()
    {
        _deliveries = [];
    }

    /// <summary>
    /// Count 的值。
    /// </summary>
    public int Count
    {
        get
        {
            return _deliveries.Count;
        }
    }

    /// <summary>
    /// 按索引获取元素。
    /// </summary>
    public MqttZeroDeliveryCandidate this[int index]
    {
        get
        {
            return _deliveries[index];
        }
    }

    /// <summary>
    /// 添加终结点。
    /// </summary>
    /// <param name="endpoint">终结点。</param>
    /// <param name="sender">对应的 sender。</param>
    internal void AddEndpoint(
        MqttZeroRouteEndpoint endpoint,
        MqttZeroSessionState? sender)
    {
        if (endpoint.RegularEntries != null)
        {
            foreach (MqttZeroPersistentHashMapEntry<
                MqttZeroSessionState,
                MqttZeroSubscription> entry in endpoint.RegularEntries)
            {
                AddRegular(entry.Key, entry.Value, sender);
            }
        }

        if (endpoint.SharedEntries != null)
        {
            foreach (MqttZeroPersistentHashMapEntry<
                MqttZeroSharedGroupKey,
                MqttZeroSharedRouteEntries> group in endpoint.SharedEntries)
            {
                AddSharedGroup(group.Value, sender);
            }
        }
    }

    /// <summary>
    /// 添加Regular。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <param name="sender">对应的 sender。</param>
    private void AddRegular(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        MqttZeroSessionState? sender)
    {
        if (subscription.NoLocal && ReferenceEquals(session, sender))
        {
            return;
        }

        _deliveries.Add(new MqttZeroDeliveryCandidate(
            session,
            subscription,
            subscription.SubscriptionIdentifiers));
    }

    /// <summary>
    /// 添加共享组。
    /// </summary>
    /// <param name="group">组。</param>
    /// <param name="sender">对应的 sender。</param>
    private void AddSharedGroup(
        MqttZeroSharedRouteEntries group,
        MqttZeroSessionState? sender)
    {
        int matchingCount = 0;
        foreach (MqttZeroPersistentHashMapEntry<
            MqttZeroSessionState,
            MqttZeroSubscription> entry in group.Entries)
        {
            if (entry.Value.NoLocal &&
                ReferenceEquals(entry.Key, sender))
            {
                continue;
            }

            matchingCount++;
        }

        if (matchingCount == 0)
        {
            return;
        }

        int selectedOffset = group.Cursor.NextOffset(matchingCount);
        foreach (MqttZeroPersistentHashMapEntry<
            MqttZeroSessionState,
            MqttZeroSubscription> entry in group.Entries)
        {
            if (entry.Value.NoLocal &&
                ReferenceEquals(entry.Key, sender))
            {
                continue;
            }

            if (selectedOffset > 0)
            {
                selectedOffset--;
                continue;
            }

            _deliveries.Add(new MqttZeroDeliveryCandidate(
                entry.Key,
                entry.Value,
                entry.Value.SubscriptionIdentifiers));
            return;
        }
    }
}
