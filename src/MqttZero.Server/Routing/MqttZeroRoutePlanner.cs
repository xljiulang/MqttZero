using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal static class MqttZeroRoutePlanner
{
    public static MqttZeroRoutePlan Plan(
        MqttZeroRouteEpoch epoch,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        MqttZeroSharedGroupCursors cursors,
        int shardCount,
        MqttZeroRoutePlanBuffer buffer)
    {
        ArgumentNullException.ThrowIfNull(epoch);
        ArgumentNullException.ThrowIfNull(cursors);
        ArgumentNullException.ThrowIfNull(buffer);
        buffer.BeginPlan(shardCount);
        PlanNode(
            epoch.Root,
            topic,
            true,
            sender,
            buffer);
        buffer.PlanSharedGroups(sender, cursors);
        return buffer.CreatePlan();
    }

    private static void AddEndpoint(
        MqttZeroRouteEndpoint endpoint,
        MqttZeroSessionState? sender,
        MqttZeroRoutePlanBuffer buffer)
    {
        if (endpoint.RegularEntries != null)
        {
            foreach (MqttZeroPersistentHashMapEntry<
                MqttZeroSessionState,
                MqttZeroSubscription> entry in endpoint.RegularEntries)
            {
                buffer.AddRegular(entry.Key, entry.Value, sender);
            }
        }

        if (endpoint.SharedEntries != null)
        {
            foreach (MqttZeroPersistentHashMapEntry<
                MqttZeroSharedGroupKey,
                MqttZeroSharedRouteEntries> group in endpoint.SharedEntries)
            {
                buffer.AddSharedGroup(group.Value);
            }
        }
    }

    private static void PlanNode(
        MqttZeroRouteTrieNode node,
        ReadOnlySpan<byte> remainingTopic,
        bool hasCurrentLevel,
        MqttZeroSessionState? sender,
        MqttZeroRoutePlanBuffer buffer)
    {
        if (node.MultiLevel?.Endpoint != null)
        {
            AddEndpoint(node.MultiLevel.Endpoint, sender, buffer);
        }

        if (!hasCurrentLevel)
        {
            if (node.Endpoint != null)
            {
                AddEndpoint(node.Endpoint, sender, buffer);
            }

            return;
        }

        int delimiter = remainingTopic.IndexOf((byte)'/');
        ReadOnlySpan<byte> level = delimiter < 0
            ? remainingTopic
            : remainingTopic[..delimiter];
        ReadOnlySpan<byte> rest = delimiter < 0
            ? ReadOnlySpan<byte>.Empty
            : remainingTopic[(delimiter + 1)..];
        bool hasMoreLevels = delimiter >= 0;

        if (node.SingleLevel != null)
        {
            PlanNode(
                node.SingleLevel,
                rest,
                hasMoreLevels,
                sender,
                buffer);
        }

        if (node.TryGetExactChild(
            level,
            out MqttZeroRouteTrieNode? exactChild))
        {
            PlanNode(
                exactChild!,
                rest,
                hasMoreLevels,
                sender,
                buffer);
        }
    }
}
