using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

/// <summary>
/// 按主题与订阅快照生成路由计划。
/// </summary>
internal static class MqttZeroRoutePlanner
{
    /// <summary>
    /// 计划。
    /// </summary>
    /// <param name="epoch">路由快照。</param>
    /// <param name="topic">主题。</param>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="shardCount">分片数量。</param>
    /// <param name="buffer">缓冲区。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroRoutePlan Plan(
        MqttZeroRouteEpoch epoch,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        int shardCount,
        MqttZeroRoutePlanBuffer buffer)
    {
        ArgumentNullException.ThrowIfNull(epoch);
        ArgumentNullException.ThrowIfNull(buffer);
        buffer.BeginPlan(shardCount);
        PlanNode(
            epoch.Root,
            topic,
            true,
            sender,
            buffer);
        buffer.PlanSharedGroups(sender);
        return buffer.CreatePlan();
    }

    /// <summary>
    /// 添加终结点。
    /// </summary>
    /// <param name="endpoint">终结点。</param>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="buffer">缓冲区。</param>
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

    /// <summary>
    /// 计划节点。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="remainingTopic">remaining主题。</param>
    /// <param name="hasCurrentLevel">has当前Level。</param>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="buffer">缓冲区。</param>
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
                exactChild,
                rest,
                hasMoreLevels,
                sender,
                buffer);
        }
    }
}
