using MqttZero.Server.Routing;

namespace MqttZero.Server.Retained;

internal sealed class MqttZeroRetainedTopicNode
{
    public static MqttZeroRetainedTopicNode Empty { get; } = new();

    private readonly MqttZeroRetainedTopicChild[] _exactChildren;

    private MqttZeroRetainedTopicNode()
    {
        _exactChildren = [];
    }

    private MqttZeroRetainedTopicNode(
        MqttZeroRetainedMessage? message,
        MqttZeroRetainedTopicChild[] exactChildren)
    {
        Message = message;
        _exactChildren = exactChildren;
    }

    public MqttZeroRetainedMessage? Message { get; }

    public ReadOnlySpan<MqttZeroRetainedTopicChild> ExactChildren
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
            return Message == null && _exactChildren.Length == 0;
        }
    }

    public MqttZeroRetainedTopicNode SetMessage(
        MqttZeroRetainedMessage message)
    {
        return ReferenceEquals(Message, message)
            ? this
            : new MqttZeroRetainedTopicNode(
                message,
                _exactChildren);
    }

    public MqttZeroRetainedTopicNode RemoveMessage()
    {
        return Message == null
            ? this
            : new MqttZeroRetainedTopicNode(null, _exactChildren);
    }

    public bool TryGetExactChild(
        ReadOnlySpan<byte> segment,
        out MqttZeroRetainedTopicNode? node)
    {
        int low = 0;
        int high = _exactChildren.Length - 1;
        while (low <= high)
        {
            int index = low + ((high - low) >> 1);
            MqttZeroRetainedTopicChild child = _exactChildren[index];
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

    public MqttZeroRetainedTopicNode SetExactChild(
        ReadOnlySpan<byte> segment,
        MqttZeroRetainedTopicNode node)
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

        MqttZeroRetainedTopicChild child = new(
            MqttZeroTopicSegment.From(segment),
            node);
        if (low < _exactChildren.Length &&
            _exactChildren[low].Segment.Span.SequenceEqual(segment))
        {
            MqttZeroRetainedTopicChild[] replacement =
                (MqttZeroRetainedTopicChild[])_exactChildren.Clone();
            replacement[low] = child;
            return new MqttZeroRetainedTopicNode(
                Message,
                replacement);
        }

        MqttZeroRetainedTopicChild[] inserted =
            new MqttZeroRetainedTopicChild[_exactChildren.Length + 1];
        _exactChildren.AsSpan(0, low).CopyTo(inserted);
        inserted[low] = child;
        _exactChildren.AsSpan(low).CopyTo(inserted.AsSpan(low + 1));
        return new MqttZeroRetainedTopicNode(Message, inserted);
    }

    public MqttZeroRetainedTopicNode? RemoveExactChild(
        ReadOnlySpan<byte> segment)
    {
        int index = 0;
        while (index < _exactChildren.Length &&
            MqttZeroTopicSegmentComparer.Compare(
                _exactChildren[index].Segment.Span,
                segment) < 0)
        {
            index++;
        }

        if (index == _exactChildren.Length ||
            !_exactChildren[index].Segment.Span.SequenceEqual(segment))
        {
            return this;
        }

        if (_exactChildren.Length == 1)
        {
            MqttZeroRetainedTopicNode result = new(
                Message,
                []);
            return result.IsEmpty ? null : result;
        }

        MqttZeroRetainedTopicChild[] replacement =
            new MqttZeroRetainedTopicChild[_exactChildren.Length - 1];
        _exactChildren.AsSpan(0, index).CopyTo(replacement);
        _exactChildren.AsSpan(index + 1).CopyTo(
            replacement.AsSpan(index));
        return new MqttZeroRetainedTopicNode(Message, replacement);
    }
}
