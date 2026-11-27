using MqttZero.Server.Routing;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace MqttZero.Server.Retained;

/// <summary>
/// 保留消息主题树节点。
/// </summary>
internal sealed class MqttZeroRetainedTopicNode
{
    /// <summary>
    /// 空。
    /// </summary>
    public static MqttZeroRetainedTopicNode Empty { get; } = new();

    private readonly MqttZeroRetainedTopicChild[] _exactChildren;

    /// <summary>
    /// 初始化 MqttZeroRetainedTopicNode 的新实例。
    /// </summary>
    private MqttZeroRetainedTopicNode()
    {
        _exactChildren = [];
    }

    /// <summary>
    /// 初始化 MqttZeroRetainedTopicNode 的新实例。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="exactChildren">对应的 exactChildren。</param>
    private MqttZeroRetainedTopicNode(
        MqttZeroRetainedMessage? message,
        MqttZeroRetainedTopicChild[] exactChildren)
    {
        Message = message;
        _exactChildren = exactChildren;
    }

    /// <summary>
    /// 消息。
    /// </summary>
    public MqttZeroRetainedMessage? Message { get; }

    /// <summary>
    /// ExactChildren 的值。
    /// </summary>
    public ReadOnlySpan<MqttZeroRetainedTopicChild> ExactChildren
    {
        get
        {
            return _exactChildren;
        }
    }

    /// <summary>
    /// IsEmpty 的值。
    /// </summary>
    public bool IsEmpty
    {
        get
        {
            return Message == null && _exactChildren.Length == 0;
        }
    }

    /// <summary>
    /// 设置消息。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRetainedTopicNode SetMessage(
        MqttZeroRetainedMessage message)
    {
        return ReferenceEquals(Message, message)
            ? this
            : new MqttZeroRetainedTopicNode(
                message,
                _exactChildren);
    }

    /// <summary>
    /// 移除消息。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroRetainedTopicNode RemoveMessage()
    {
        return Message == null
            ? this
            : new MqttZeroRetainedTopicNode(null, _exactChildren);
    }

    /// <summary>
    /// 尝试GetExactChild。
    /// </summary>
    /// <param name="segment">段。</param>
    /// <param name="node">节点。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool TryGetExactChild(
        ReadOnlySpan<byte> segment,
        [NotNullWhen(true)] out MqttZeroRetainedTopicNode? node)
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

    /// <summary>
    /// 设置ExactChild。
    /// </summary>
    /// <param name="segment">段。</param>
    /// <param name="node">节点。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 移除ExactChild。
    /// </summary>
    /// <param name="segment">段。</param>
    /// <returns>对应的结果。</returns>
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
