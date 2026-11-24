namespace MqttZero.Server.Routing;

internal sealed class MqttZeroTopicTrie
{
    private readonly Node _root = new();

    public MqttZeroTopicTrie(MqttZeroRouteFilterGroup[] groups)
    {
        for (int groupIndex = 0; groupIndex < groups.Length; groupIndex++)
        {
            Add(groups[groupIndex].Filter, groupIndex);
        }
    }

    public void Match(
        ReadOnlySpan<byte> topic,
        MqttZeroRouteScope route)
    {
        MatchNode(
            _root,
            topic,
            true,
            route);
    }

    private void Add(ReadOnlySpan<byte> filter, int groupIndex)
    {
        Node node = _root;
        int offset = 0;
        while (offset <= filter.Length)
        {
            int delimiter = filter[offset..].IndexOf((byte)'/');
            int end = delimiter < 0 ? filter.Length : offset + delimiter;
            ReadOnlySpan<byte> level = filter[offset..end];
            if (level.Length == 1 && level[0] == (byte)'#')
            {
                node.MultiLevelGroups ??= [];
                node.MultiLevelGroups.Add(groupIndex);
                return;
            }

            if (level.Length == 1 && level[0] == (byte)'+')
            {
                node.SingleLevel ??= new Node();
                node = node.SingleLevel;
            }
            else
            {
                node = node.GetOrAddChild(level);
            }

            if (delimiter < 0)
            {
                node.ExactGroups ??= [];
                node.ExactGroups.Add(groupIndex);
                return;
            }

            offset = end + 1;
        }
    }

    private static void MatchNode(
        Node node,
        ReadOnlySpan<byte> remainingTopic,
        bool hasCurrentLevel,
        MqttZeroRouteScope route)
    {
        AddGroups(node.MultiLevelGroups, route);
        if (!hasCurrentLevel)
        {
            AddGroups(node.ExactGroups, route);
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

        if (node.Children.TryGetValue(
                Hash(level),
                out List<Child>? children))
        {
            for (int index = 0; index < children.Count; index++)
            {
                Child child = children[index];
                if (child.Segment.AsSpan().SequenceEqual(level))
                {
                    MatchNode(
                        child.Node,
                        rest,
                        hasMoreLevels,
                        route);
                }
            }
        }

        if (node.SingleLevel != null)
        {
            MatchNode(
                node.SingleLevel,
                rest,
                hasMoreLevels,
                route);
        }
    }

    private static void AddGroups(
        List<int>? groups,
        MqttZeroRouteScope route)
    {
        if (groups == null)
        {
            return;
        }

        for (int index = 0; index < groups.Count; index++)
        {
            route.AddMatchedGroupIndex(groups[index]);
        }
    }

    private static int Hash(ReadOnlySpan<byte> value)
    {
        uint hash = 2166136261;
        for (int index = 0; index < value.Length; index++)
        {
            hash = (hash ^ value[index]) * 16777619;
        }

        return (int)hash;
    }

    private sealed class Node
    {
        public Dictionary<int, List<Child>> Children { get; } = [];

        public List<int>? ExactGroups { get; set; }

        public List<int>? MultiLevelGroups { get; set; }

        public Node? SingleLevel { get; set; }

        public Node GetOrAddChild(ReadOnlySpan<byte> segment)
        {
            int hash = Hash(segment);
            if (!Children.TryGetValue(hash, out List<Child>? children))
            {
                children = [];
                Children[hash] = children;
            }

            for (int index = 0; index < children.Count; index++)
            {
                Child child = children[index];
                if (child.Segment.AsSpan().SequenceEqual(segment))
                {
                    return child.Node;
                }
            }

            Node created = new();
            children.Add(new Child(segment.ToArray(), created));
            return created;
        }
    }

    private sealed class Child
    {
        public Child(byte[] segment, Node node)
        {
            Segment = segment;
            Node = node;
        }

        public byte[] Segment { get; }

        public Node Node { get; }
    }
}
