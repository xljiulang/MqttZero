using MqttZero.Server.Routing;

namespace MqttZero.Server.Retained;

/// <summary>
/// 保留消息的主题前缀树。
/// </summary>
internal sealed class MqttZeroRetainedTopicTrie
{
    /// <summary>
    /// 空。
    /// </summary>
    public static MqttZeroRetainedTopicTrie Empty { get; } = new(MqttZeroRetainedTopicNode.Empty);

    /// <summary>
    /// 初始化 MqttZeroRetainedTopicTrie 的新实例。
    /// </summary>
    /// <param name="root">对应的 root。</param>
    private MqttZeroRetainedTopicTrie(
        MqttZeroRetainedTopicNode root)
    {
        Root = root;
    }

    /// <summary>
    /// Root 的值。
    /// </summary>
    internal MqttZeroRetainedTopicNode Root { get; }

    /// <summary>
    /// 设置。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="message">消息。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRetainedTopicTrie Set(
        ReadOnlySpan<byte> topic,
        MqttZeroRetainedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        MqttZeroRetainedTopicNode root = SetNode(
            Root,
            topic,
            0,
            true,
            message);
        return ReferenceEquals(root, Root)
            ? this
            : new MqttZeroRetainedTopicTrie(root);
    }

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRetainedTopicTrie Remove(ReadOnlySpan<byte> topic)
    {
        MqttZeroRetainedTopicNode? root = RemoveNode(
            Root,
            topic,
            0,
            true);
        if (ReferenceEquals(root, Root))
        {
            return this;
        }

        return root == null
            ? Empty
            : new MqttZeroRetainedTopicTrie(root);
    }

    /// <summary>
    /// 匹配。
    /// </summary>
    /// <param name="filter">过滤器。</param>
    /// <param name="results">结果。</param>
    public void Match(
        ReadOnlySpan<byte> filter,
        List<MqttZeroRetainedMessage> results)
    {
        ArgumentNullException.ThrowIfNull(results);
        MatchNode(
            Root,
            filter,
            0,
            true,
            results);
    }

    /// <summary>
    /// Collect消息。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="results">结果。</param>
    private static void CollectMessages(
        MqttZeroRetainedTopicNode node,
        List<MqttZeroRetainedMessage> results)
    {
        if (node.Message != null)
        {
            results.Add(node.Message);
        }

        ReadOnlySpan<MqttZeroRetainedTopicChild> children =
            node.ExactChildren;
        for (int index = 0; index < children.Length; index++)
        {
            CollectMessages(children[index].Node, results);
        }
    }

    /// <summary>
    /// 匹配节点。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="filter">过滤器。</param>
    /// <param name="offset">偏移。</param>
    /// <param name="hasCurrentLevel">has当前Level。</param>
    /// <param name="results">结果。</param>
    private static void MatchNode(
        MqttZeroRetainedTopicNode node,
        ReadOnlySpan<byte> filter,
        int offset,
        bool hasCurrentLevel,
        List<MqttZeroRetainedMessage> results)
    {
        if (!hasCurrentLevel)
        {
            if (node.Message != null)
            {
                results.Add(node.Message);
            }

            return;
        }

        int end = offset < filter.Length
            ? filter[offset..].IndexOf((byte)'/')
            : -1;
        end = end < 0 ? filter.Length : offset + end;
        ReadOnlySpan<byte> level = filter[offset..end];
        bool hasMoreLevels = end < filter.Length;
        int nextOffset = hasMoreLevels ? end + 1 : end;

        if (level.Length == 1 && level[0] == (byte)'#')
        {
            CollectMessages(node, results);
            return;
        }

        if (level.Length == 1 && level[0] == (byte)'+')
        {
            ReadOnlySpan<MqttZeroRetainedTopicChild> children =
                node.ExactChildren;
            for (int index = 0; index < children.Length; index++)
            {
                MatchNode(
                    children[index].Node,
                    filter,
                    nextOffset,
                    hasMoreLevels,
                    results);
            }

            return;
        }

        if (node.TryGetExactChild(
            level,
            out MqttZeroRetainedTopicNode? child))
        {
            MatchNode(
                child,
                filter,
                nextOffset,
                hasMoreLevels,
                results);
        }
    }

    /// <summary>
    /// 移除节点。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="topic">主题。</param>
    /// <param name="offset">偏移。</param>
    /// <param name="hasCurrentLevel">has当前Level。</param>
    /// <returns>对应的结果。</returns>
    private static MqttZeroRetainedTopicNode? RemoveNode(
        MqttZeroRetainedTopicNode node,
        ReadOnlySpan<byte> topic,
        int offset,
        bool hasCurrentLevel)
    {
        if (!hasCurrentLevel)
        {
            if (node.Message == null)
            {
                return node;
            }

            MqttZeroRetainedTopicNode result = node.RemoveMessage();
            return result.IsEmpty ? null : result;
        }

        int end = offset < topic.Length
            ? topic[offset..].IndexOf((byte)'/')
            : -1;
        end = end < 0 ? topic.Length : offset + end;
        ReadOnlySpan<byte> level = topic[offset..end];
        bool hasMoreLevels = end < topic.Length;
        int nextOffset = hasMoreLevels ? end + 1 : end;
        if (!node.TryGetExactChild(
            level,
            out MqttZeroRetainedTopicNode? child))
        {
            return node;
        }

        MqttZeroRetainedTopicNode? replacement = RemoveNode(
            child,
            topic,
            nextOffset,
            hasMoreLevels);
        if (ReferenceEquals(replacement, child))
        {
            return node;
        }

        return replacement == null
            ? node.RemoveExactChild(level)
            : node.SetExactChild(level, replacement);
    }

    /// <summary>
    /// 设置节点。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="topic">主题。</param>
    /// <param name="offset">偏移。</param>
    /// <param name="hasCurrentLevel">has当前Level。</param>
    /// <param name="message">消息。</param>
    /// <returns>对应的结果。</returns>
    private static MqttZeroRetainedTopicNode SetNode(
        MqttZeroRetainedTopicNode node,
        ReadOnlySpan<byte> topic,
        int offset,
        bool hasCurrentLevel,
        MqttZeroRetainedMessage message)
    {
        if (!hasCurrentLevel)
        {
            return node.SetMessage(message);
        }

        int end = offset < topic.Length
            ? topic[offset..].IndexOf((byte)'/')
            : -1;
        end = end < 0 ? topic.Length : offset + end;
        ReadOnlySpan<byte> level = topic[offset..end];
        bool hasMoreLevels = end < topic.Length;
        int nextOffset = hasMoreLevels ? end + 1 : end;
        MqttZeroRetainedTopicNode child = node.TryGetExactChild(
            level,
            out MqttZeroRetainedTopicNode? existing)
                ? existing!
                : MqttZeroRetainedTopicNode.Empty;
        MqttZeroRetainedTopicNode replacement = SetNode(
            child,
            topic,
            nextOffset,
            hasMoreLevels,
            message);
        return ReferenceEquals(replacement, child)
            ? node
            : node.SetExactChild(level, replacement);
    }
}
