using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRouteEpoch
{
    public static MqttZeroRouteEpoch Empty { get; } = new(
        0,
        MqttZeroRouteTrieNode.Empty);

    public MqttZeroRouteEpoch(
        long version,
        MqttZeroRouteTrieNode root)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(version);
        Version = version;
        Root = root ?? throw new ArgumentNullException(nameof(root));
    }

    public long Version { get; }

    internal MqttZeroRouteTrieNode Root { get; }

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
            ? new MqttZeroRouteEpoch(Version + 1, root)
            : this;
    }

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
                root ?? MqttZeroRouteTrieNode.Empty)
            : this;
    }

    public void Match(
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        MqttZeroSharedGroupCursors cursors,
        ref MqttZeroRouteEpochMatch match)
    {
        ArgumentNullException.ThrowIfNull(cursors);
        MatchNode(Root, topic, true, sender, cursors, ref match);
    }

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
                node = child!;
            }

            if (delimiter < 0)
            {
                return node;
            }

            offset = end + 1;
        }

        return node;
    }

    private static MqttZeroRouteTrieNode Update(
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

    private static MqttZeroRouteTrieNode? UpdateOrNull(
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
            sharedKey = new MqttZeroSharedGroupKey(
                Encoding.UTF8.GetBytes(subscription.SharedGroup!),
                filter);
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

    private static MqttZeroRouteTrieNode? UpdateNode(
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

    private static MqttZeroRouteTrieNode? UpdateEndpoint(
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
                ? endpoint.SetShared(sharedKey, session, subscription)
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

    private static void MatchNode(
        MqttZeroRouteTrieNode node,
        ReadOnlySpan<byte> remainingTopic,
        bool hasCurrentLevel,
        MqttZeroSessionState? sender,
        MqttZeroSharedGroupCursors cursors,
        ref MqttZeroRouteEpochMatch match)
    {
        if (node.MultiLevel?.Endpoint != null)
        {
            match.AddEndpoint(node.MultiLevel.Endpoint, sender, cursors);
        }

        if (!hasCurrentLevel)
        {
            if (node.Endpoint != null)
            {
                match.AddEndpoint(node.Endpoint, sender, cursors);
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
                exactChild!,
                rest,
                hasMoreLevels,
                sender,
                cursors,
                ref match);
        }

        if (node.SingleLevel != null)
        {
            MatchNode(
                node.SingleLevel,
                rest,
                hasMoreLevels,
                sender,
                cursors,
                ref match);
        }
    }

}
