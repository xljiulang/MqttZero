using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Server.Routing;

/// <summary>
/// 不可变的订阅路由快照。
/// </summary>
internal sealed class MqttZeroRouteEpoch
{
    /// <summary>
    /// Empty 的值。
    /// </summary>
    public static MqttZeroRouteEpoch Empty
    {
        get
        {
            return new MqttZeroRouteEpoch(
                0,
                MqttZeroRouteTrieNode.Empty,
                new MqttZeroSharedGroupCursors());
        }
    }

    /// <summary>
    /// 初始化 MqttZeroRouteEpoch 的新实例。
    /// </summary>
    /// <param name="version">版本。</param>
    /// <param name="root">对应的 root。</param>
    public MqttZeroRouteEpoch(
        long version,
        MqttZeroRouteTrieNode root)
        : this(version, root, new MqttZeroSharedGroupCursors())
    {
    }

    /// <summary>
    /// 初始化 MqttZeroRouteEpoch 的新实例。
    /// </summary>
    /// <param name="version">版本。</param>
    /// <param name="root">对应的 root。</param>
    /// <param name="cursors">游标。</param>
    public MqttZeroRouteEpoch(
        long version,
        MqttZeroRouteTrieNode root,
        MqttZeroSharedGroupCursors cursors)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(version);
        Version = version;
        Root = root ?? throw new ArgumentNullException(nameof(root));
        Cursors = cursors ?? throw new ArgumentNullException(nameof(cursors));
    }

    /// <summary>
    /// 版本。
    /// </summary>
    public long Version { get; }

    /// <summary>
    /// Root 的值。
    /// </summary>
    internal MqttZeroRouteTrieNode Root { get; }

    /// <summary>
    /// 游标。
    /// </summary>
    internal MqttZeroSharedGroupCursors Cursors { get; }

    /// <summary>
    /// 添加。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteEpoch Add(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(subscription);
        MqttZeroRouteTrieNode root = Update(
            Root,
            subscription,
            session,
            add: true,
            out bool changed);
        return changed
            ? new MqttZeroRouteEpoch(Version + 1, root, Cursors)
            : this;
    }

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteEpoch Remove(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(subscription);
        MqttZeroRouteTrieNode? root = UpdateOrNull(
            Root,
            subscription,
            session,
            out bool changed);
        return changed
            ? new MqttZeroRouteEpoch(
                Version + 1,
                root ?? MqttZeroRouteTrieNode.Empty,
                Cursors)
            : this;
    }

    /// <summary>
    /// Matches a topic against this epoch and collects delivery candidates.
    /// </summary>
    /// <param name="topic">The published topic.</param>
    /// <param name="sender">The publishing session, or null for server-originated messages.</param>
    /// <param name="match">The match accumulator.</param>
    /// <remarks>
    /// Test and benchmark only. Production routing uses
    /// <see cref="MqttZeroRoutePlanner"/> with <see cref="MqttZeroRoutePlanBuffer"/>,
    /// which plans per-shard batches directly.
    /// </remarks>
    public void Match(
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        ref MqttZeroRouteEpochMatch match)
    {
        MatchNode(Root, topic, true, sender, ref match);
    }

    /// <summary>
    /// Collect共享组键。
    /// </summary>
    /// <param name="keys">键。</param>
    public void CollectSharedGroupKeys(
        HashSet<MqttZeroSharedGroupKey> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        Root.CollectSharedGroupKeys(keys);
    }

    /// <summary>
    /// 获取节点Test。
    /// </summary>
    /// <param name="filter">过滤器。</param>
    /// <returns>对应的结果。</returns>
    internal MqttZeroRouteTrieNode? GetNodeForTest(
        ReadOnlySpan<byte> filter)
    {
        MqttZeroRouteTrieNode node = Root;
        int offset = 0;
        while (offset <= filter.Length)
        {
            int delimiter = filter[offset..].IndexOf((byte)'/');
            int end = delimiter < 0 ? filter.Length : offset + delimiter;
            ReadOnlySpan<byte> level = filter[offset..end];
            if (level.Length == 1 && level[0] == (byte)'#')
            {
                return node.MultiLevel;
            }

            if (level.Length == 1 && level[0] == (byte)'+')
            {
                MqttZeroRouteTrieNode? singleLevel = node.SingleLevel;
                if (singleLevel == null)
                {
                    return null;
                }

                node = singleLevel;
            }
            else if (!node.TryGetExactChild(level, out MqttZeroRouteTrieNode? child))
            {
                return null;
            }
            else
            {
                node = child;
            }

            if (delimiter < 0)
            {
                return node;
            }

            offset = end + 1;
        }

        return node;
    }

    /// <summary>
    /// 更新。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="subscription">订阅。</param>
    /// <param name="session">会话。</param>
    /// <param name="add">对应的 add。</param>
    /// <param name="changed">对应的 changed。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroRouteTrieNode Update(
        MqttZeroRouteTrieNode node,
        MqttZeroSubscription subscription,
        MqttZeroSessionState session,
        bool add,
        out bool changed)
    {
        MqttZeroRouteTrieNode? result = UpdateOrNull(
            node,
            subscription,
            session,
            out changed,
            add);
        return result ?? MqttZeroRouteTrieNode.Empty;
    }

    /// <summary>
    /// 更新OrNull。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="subscription">订阅。</param>
    /// <param name="session">会话。</param>
    /// <param name="changed">对应的 changed。</param>
    /// <param name="add">对应的 add。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroRouteTrieNode? UpdateOrNull(
        MqttZeroRouteTrieNode node,
        MqttZeroSubscription subscription,
        MqttZeroSessionState session,
        out bool changed,
        bool add = false)
    {
        ReadOnlySpan<byte> filter = subscription.MatchFilter;
        MqttZeroSharedGroupKey sharedKey = default;
        bool shared = subscription.IsShared;
        if (shared)
        {
            sharedKey = MqttZeroSharedGroupKey.Create(subscription);
        }

        return UpdateNode(
            node,
            filter,
            0,
            session,
            subscription,
            shared,
            sharedKey,
            add,
            out changed);
    }

    /// <summary>
    /// 更新节点。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="filter">过滤器。</param>
    /// <param name="offset">偏移。</param>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <param name="shared">共享。</param>
    /// <param name="sharedKey">共享键。</param>
    /// <param name="add">对应的 add。</param>
    /// <param name="changed">对应的 changed。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroRouteTrieNode? UpdateNode(
        MqttZeroRouteTrieNode node,
        ReadOnlySpan<byte> filter,
        int offset,
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        bool shared,
        MqttZeroSharedGroupKey sharedKey,
        bool add,
        out bool changed)
    {
        if (offset > filter.Length)
        {
            changed = false;
            return node;
        }

        if (offset == filter.Length)
        {
            return UpdateEndpoint(
                node,
                session,
                subscription,
                shared,
                sharedKey,
                add,
                out changed);
        }

        int delimiter = filter[offset..].IndexOf((byte)'/');
        int end = delimiter < 0 ? filter.Length : offset + delimiter;
        ReadOnlySpan<byte> level = filter[offset..end];
        int nextOffset = delimiter < 0 ? filter.Length : end + 1;
        if (level.Length == 1 && level[0] == (byte)'#')
        {
            MqttZeroRouteTrieNode wildcard = node.MultiLevel ??
                MqttZeroRouteTrieNode.Empty;
            MqttZeroRouteTrieNode? replacement = UpdateEndpoint(
                wildcard,
                session,
                subscription,
                shared,
                sharedKey,
                add,
                out changed);
            if (!changed)
            {
                return node;
            }

            MqttZeroRouteTrieNode wildcardResult = node.WithMultiLevel(
                replacement == null || replacement.IsEmpty
                    ? null
                    : replacement);
            return wildcardResult.IsEmpty ? null : wildcardResult;
        }

        if (level.Length == 1 && level[0] == (byte)'+')
        {
            MqttZeroRouteTrieNode singleLevel = node.SingleLevel ??
                MqttZeroRouteTrieNode.Empty;
            MqttZeroRouteTrieNode? replacement = UpdateNode(
                singleLevel,
                filter,
                nextOffset,
                session,
                subscription,
                shared,
                sharedKey,
                add,
                out changed);
            if (!changed)
            {
                return node;
            }

            MqttZeroRouteTrieNode singleResult = node.WithSingleLevel(
                replacement == null || replacement.IsEmpty
                    ? null
                    : replacement);
            return singleResult.IsEmpty ? null : singleResult;
        }

        MqttZeroRouteTrieNode child = node.TryGetExactChild(
            level,
            out MqttZeroRouteTrieNode? existing)
                ? existing!
                : MqttZeroRouteTrieNode.Empty;
        MqttZeroRouteTrieNode? childReplacement = UpdateNode(
            child,
            filter,
            nextOffset,
            session,
            subscription,
            shared,
            sharedKey,
            add,
            out changed);
        if (!changed)
        {
            return node;
        }

        MqttZeroRouteTrieNode result = childReplacement == null ||
            childReplacement.IsEmpty
                ? node.RemoveExactChild(level)
                : node.SetExactChild(level, childReplacement);
        return result.IsEmpty ? null : result;
    }

    /// <summary>
    /// 更新终结点。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <param name="shared">共享。</param>
    /// <param name="sharedKey">共享键。</param>
    /// <param name="add">对应的 add。</param>
    /// <param name="changed">对应的 changed。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroRouteTrieNode? UpdateEndpoint(
        MqttZeroRouteTrieNode node,
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        bool shared,
        MqttZeroSharedGroupKey sharedKey,
        bool add,
        out bool changed)
    {
        MqttZeroRouteEndpoint endpoint = node.Endpoint ??
            MqttZeroRouteEndpoint.Empty;
        MqttZeroRouteEndpoint? replacement;
        if (add)
        {
            replacement = shared
                ? endpoint.SetShared(
                    sharedKey,
                    session,
                    subscription,
                    Cursors.Resolve(sharedKey))
                : endpoint.SetRegular(session, subscription);
            changed = true;
        }
        else
        {
            replacement = shared
                ? endpoint.RemoveShared(
                    sharedKey,
                    session,
                    out changed)
                : endpoint.RemoveRegular(session, out changed);
        }

        if (!changed)
        {
            return node;
        }

        MqttZeroRouteTrieNode result = node.WithEndpoint(replacement);
        return result.IsEmpty ? null : result;
    }

    /// <summary>
    /// 匹配节点。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="remainingTopic">remaining主题。</param>
    /// <param name="hasCurrentLevel">has当前Level。</param>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="match">对应的 match。</param>
    private static void MatchNode(
        MqttZeroRouteTrieNode node,
        ReadOnlySpan<byte> remainingTopic,
        bool hasCurrentLevel,
        MqttZeroSessionState? sender,
        ref MqttZeroRouteEpochMatch match)
    {
        if (node.MultiLevel?.Endpoint != null)
        {
            match.AddEndpoint(node.MultiLevel.Endpoint, sender);
        }

        if (!hasCurrentLevel)
        {
            if (node.Endpoint != null)
            {
                match.AddEndpoint(node.Endpoint, sender);
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

        if (node.TryGetExactChild(
                level,
                out MqttZeroRouteTrieNode? exactChild))
        {
            MatchNode(
                exactChild,
                rest,
                hasMoreLevels,
                sender,
                ref match);
        }

        if (node.SingleLevel != null)
        {
            MatchNode(
                node.SingleLevel,
                rest,
                hasMoreLevels,
                sender,
                ref match);
        }
    }

}
