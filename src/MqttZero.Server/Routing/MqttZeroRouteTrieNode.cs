namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRouteTrieNode
{
    public static MqttZeroRouteTrieNode Empty { get; } = new();

    private readonly MqttZeroRouteChild[] _exactChildren;

    private MqttZeroRouteTrieNode()
    {
        _exactChildren = [];
    }

    private MqttZeroRouteTrieNode(
        MqttZeroRouteEndpoint? endpoint,
        MqttZeroRouteTrieNode? singleLevel,
        MqttZeroRouteTrieNode? multiLevel,
        MqttZeroRouteChild[] exactChildren)
    {
        Endpoint = endpoint;
        SingleLevel = singleLevel;
        MultiLevel = multiLevel;
        _exactChildren = exactChildren;
    }

    public MqttZeroRouteEndpoint? Endpoint { get; }

    public MqttZeroRouteTrieNode? SingleLevel { get; }

    public MqttZeroRouteTrieNode? MultiLevel { get; }

    public ReadOnlySpan<MqttZeroRouteChild> ExactChildren
    {
        get
        {
            return _exactChildren;
        }
    }

    public bool IsEmpty
    {
        get
        {
            return Endpoint == null &&
                SingleLevel == null &&
                MultiLevel == null &&
                _exactChildren.Length == 0;
        }
    }

    public MqttZeroRouteTrieNode WithEndpoint(
        MqttZeroRouteEndpoint? endpoint)
    {
        return new MqttZeroRouteTrieNode(
            endpoint,
            SingleLevel,
            MultiLevel,
            _exactChildren);
    }

    public MqttZeroRouteTrieNode WithSingleLevel(
        MqttZeroRouteTrieNode? singleLevel)
    {
        return new MqttZeroRouteTrieNode(
            Endpoint,
            singleLevel,
            MultiLevel,
            _exactChildren);
    }

    public MqttZeroRouteTrieNode WithMultiLevel(
        MqttZeroRouteTrieNode? multiLevel)
    {
        return new MqttZeroRouteTrieNode(
            Endpoint,
            SingleLevel,
            multiLevel,
            _exactChildren);
    }

    public bool TryGetExactChild(
        ReadOnlySpan<byte> segment,
        out MqttZeroRouteTrieNode? node)
    {
        int low = 0;
        int high = _exactChildren.Length - 1;
        while (low <= high)
        {
            int index = low + ((high - low) >> 1);
            MqttZeroRouteChild child = _exactChildren[index];
            int comparison = MqttZeroTopicSegmentComparer.Compare(
                child.Segment.Span,
                segment);
            if (comparison == 0)
            {
                node = child.Node;
                return true;
            }

            if (comparison < 0)
            {
                low = index + 1;
            }
            else
            {
                high = index - 1;
            }
        }

        node = null;
        return false;
    }

    public MqttZeroRouteTrieNode SetExactChild(
        ReadOnlySpan<byte> segment,
        MqttZeroRouteTrieNode node)
    {
        int low = 0;
        int high = _exactChildren.Length;
        while (low < high)
        {
            int index = low + ((high - low) >> 1);
            if (MqttZeroTopicSegmentComparer.Compare(
                    _exactChildren[index].Segment.Span,
                    segment) < 0)
            {
                low = index + 1;
            }
            else
            {
                high = index;
            }
        }

        MqttZeroRouteChild child = new(
            MqttZeroTopicSegment.From(segment),
            node);
        if (low < _exactChildren.Length &&
            _exactChildren[low].Segment.Span.SequenceEqual(segment))
        {
            MqttZeroRouteChild[] replacement = (MqttZeroRouteChild[])
                _exactChildren.Clone();
            replacement[low] = child;
            return new MqttZeroRouteTrieNode(
                Endpoint,
                SingleLevel,
                MultiLevel,
                replacement);
        }

        MqttZeroRouteChild[] inserted = new MqttZeroRouteChild[
            _exactChildren.Length + 1];
        _exactChildren.AsSpan(0, low).CopyTo(inserted);
        inserted[low] = child;
        _exactChildren.AsSpan(low).CopyTo(inserted.AsSpan(low + 1));
        return new MqttZeroRouteTrieNode(
            Endpoint,
            SingleLevel,
            MultiLevel,
            inserted);
    }

    public MqttZeroRouteTrieNode RemoveExactChild(ReadOnlySpan<byte> segment)
    {
        int index = 0;
        while (index < _exactChildren.Length)
        {
            int comparison = MqttZeroTopicSegmentComparer.Compare(
                _exactChildren[index].Segment.Span,
                segment);
            if (comparison >= 0)
            {
                break;
            }

            index++;
        }

        if (_exactChildren.Length == 1)
        {
            return new MqttZeroRouteTrieNode(
                Endpoint,
                SingleLevel,
                MultiLevel,
                []);
        }

        MqttZeroRouteChild[] replacement = new MqttZeroRouteChild[
            _exactChildren.Length - 1];
        _exactChildren.AsSpan(0, index).CopyTo(replacement);
        _exactChildren.AsSpan(index + 1).CopyTo(
            replacement.AsSpan(index));
        return new MqttZeroRouteTrieNode(
            Endpoint,
            SingleLevel,
            MultiLevel,
            replacement);
    }
}
