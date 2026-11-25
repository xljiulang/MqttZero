using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal ref struct MqttZeroRouteEpochMatch
{
    private readonly List<MqttZeroDeliveryCandidate> _deliveries;

    public MqttZeroRouteEpochMatch()
    {
        _deliveries = [];
    }

    public int Count
    {
        get
        {
            return _deliveries.Count;
        }
    }

    public MqttZeroDeliveryCandidate this[int index]
    {
        get
        {
            return _deliveries[index];
        }
    }

    internal void AddEndpoint(
        MqttZeroRouteEndpoint endpoint,
        MqttZeroSessionState? sender,
        MqttZeroSharedGroupCursors cursors)
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
                AddSharedGroup(group.Value, sender, cursors);
            }
        }
    }

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

    private void AddSharedGroup(
        MqttZeroSharedRouteEntries group,
        MqttZeroSessionState? sender,
        MqttZeroSharedGroupCursors cursors)
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

        int selectedOffset = cursors.GetNext(group.Key, matchingCount);
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
