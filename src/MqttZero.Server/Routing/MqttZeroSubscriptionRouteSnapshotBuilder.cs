using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Server.Routing;

internal static class MqttZeroSubscriptionRouteSnapshotBuilder
{
    public static MqttZeroSubscriptionRouteSnapshot Build(
        IEnumerable<MqttZeroSessionState> sessions,
        MqttZeroSubscriptionRouteSnapshot? previousSnapshot = null)
    {
        Dictionary<string, List<MqttZeroRouteEntry>> regularGroups =
            new(StringComparer.Ordinal);
        Dictionary<string, List<MqttZeroRouteEntry>> sharedGroups =
            new(StringComparer.Ordinal);
        foreach (MqttZeroSessionState session in sessions)
        {
            lock (session.SyncRoot)
            {
                foreach (MqttZeroSubscription subscription in session.Subscriptions)
                {
                    MqttZeroRouteEntry entry = new(session, subscription);
                    if (subscription.IsShared)
                    {
                        AddEntry(
                            sharedGroups,
                            CreateSharedSubscriptionKey(
                                subscription.SharedGroup!,
                                subscription.MatchFilter),
                            entry);
                        continue;
                    }

                    AddEntry(
                        regularGroups,
                        Encoding.UTF8.GetString(subscription.MatchFilter),
                        entry);
                }
            }
        }

        MqttZeroRouteFilterGroup[] regularRouteGroups =
            new MqttZeroRouteFilterGroup[regularGroups.Count];
        int regularGroupIndex = 0;
        foreach (List<MqttZeroRouteEntry> entries in regularGroups.Values)
        {
            regularRouteGroups[regularGroupIndex++] = new MqttZeroRouteFilterGroup(
                entries[0].Subscription.MatchFilter,
                entries.ToArray());
        }

        MqttZeroSharedRouteGroup[] sharedRouteGroups =
            new MqttZeroSharedRouteGroup[sharedGroups.Count];
        int sharedGroupIndex = 0;
        foreach (KeyValuePair<string, List<MqttZeroRouteEntry>> pair in sharedGroups)
        {
            MqttZeroSharedRouteEntry[] entries = BuildSharedEntries(pair.Value);
            int initialCursor = previousSnapshot?.SharedGroups
                .FirstOrDefault(group => group.CursorKey == pair.Key)
                ?.Cursor ?? 0;
            sharedRouteGroups[sharedGroupIndex++] = new MqttZeroSharedRouteGroup(
                pair.Value[0].Subscription.SharedGroup!,
                entries,
                pair.Key,
                initialCursor);
        }

        return new MqttZeroSubscriptionRouteSnapshot(
            regularRouteGroups,
            sharedRouteGroups);
    }

    private static void AddEntry(
        Dictionary<string, List<MqttZeroRouteEntry>> groups,
        string key,
        MqttZeroRouteEntry entry)
    {
        if (!groups.TryGetValue(key, out List<MqttZeroRouteEntry>? entries))
        {
            entries = [];
            groups[key] = entries;
        }

        entries.Add(entry);
    }

    private static string CreateSharedSubscriptionKey(
        string group,
        ReadOnlySpan<byte> filter)
    {
        return string.Concat(group, "\0", Encoding.UTF8.GetString(filter));
    }

    private static MqttZeroSharedRouteEntry[] BuildSharedEntries(
        List<MqttZeroRouteEntry> entries)
    {
        Dictionary<MqttZeroSessionState, List<MqttZeroSubscription>>
            subscriptionsBySession = [];
        foreach (MqttZeroRouteEntry entry in entries)
        {
            if (!subscriptionsBySession.TryGetValue(
                entry.Session,
                out List<MqttZeroSubscription>? subscriptions))
            {
                subscriptions = [];
                subscriptionsBySession[entry.Session] = subscriptions;
            }

            subscriptions.Add(entry.Subscription);
        }

        MqttZeroSharedRouteEntry[] result =
            new MqttZeroSharedRouteEntry[subscriptionsBySession.Count];
        int index = 0;
        foreach (KeyValuePair<MqttZeroSessionState, List<MqttZeroSubscription>> entry in
            subscriptionsBySession)
        {
            result[index++] = new MqttZeroSharedRouteEntry(
                entry.Key,
                entry.Value.ToArray());
        }

        return result;
    }
}
