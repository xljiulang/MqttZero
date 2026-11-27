using System.Diagnostics.CodeAnalysis;

namespace MqttZero.Server.Routing;

/// <summary>
/// 主题段前缀树节点。
/// </summary>
internal sealed class MqttZeroRouteTrieNode
{
    /// <summary>
    /// 空。
    /// </summary>
    public static MqttZeroRouteTrieNode Empty { get; } = new();

    private readonly MqttZeroRouteChild[] _exactChildren;

    /// <summary>
    /// 初始化 MqttZeroRouteTrieNode 的新实例。
    /// </summary>
    private MqttZeroRouteTrieNode()
    {
        _exactChildren = [];
    }

    /// <summary>
    /// 初始化 MqttZeroRouteTrieNode 的新实例。
    /// </summary>
    /// <param name="endpoint">终结点。</param>
    /// <param name="singleLevel">对应的 singleLevel。</param>
    /// <param name="multiLevel">对应的 multiLevel。</param>
    /// <param name="exactChildren">对应的 exactChildren。</param>
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

    /// <summary>
    /// 终结点。
    /// </summary>
    public MqttZeroRouteEndpoint? Endpoint { get; }

    /// <summary>
    /// SingleLevel 的值。
    /// </summary>
    public MqttZeroRouteTrieNode? SingleLevel { get; }

    /// <summary>
    /// MultiLevel 的值。
    /// </summary>
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

    /// <summary>
    /// Collect共享组键。
    /// </summary>
    /// <param name="keys">键。</param>
    public void CollectSharedGroupKeys(
        HashSet<MqttZeroSharedGroupKey> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        Endpoint?.CollectSharedGroupKeys(keys);
        SingleLevel?.CollectSharedGroupKeys(keys);
        MultiLevel?.CollectSharedGroupKeys(keys);
        for (int index = 0; index < _exactChildren.Length; index++)
        {
            _exactChildren[index].Node.CollectSharedGroupKeys(keys);
        }
    }

    /// <summary>
    /// 附带终结点。
    /// </summary>
    /// <param name="endpoint">终结点。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteTrieNode WithEndpoint(
        MqttZeroRouteEndpoint? endpoint)
    {
        return new MqttZeroRouteTrieNode(
            endpoint,
            SingleLevel,
            MultiLevel,
            _exactChildren);
    }

    /// <summary>
    /// 附带SingleLevel。
    /// </summary>
    /// <param name="singleLevel">对应的 singleLevel。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteTrieNode WithSingleLevel(
        MqttZeroRouteTrieNode? singleLevel)
    {
        return new MqttZeroRouteTrieNode(
            Endpoint,
            singleLevel,
            MultiLevel,
            _exactChildren);
    }

    /// <summary>
    /// 附带MultiLevel。
    /// </summary>
    /// <param name="multiLevel">对应的 multiLevel。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteTrieNode WithMultiLevel(
        MqttZeroRouteTrieNode? multiLevel)
    {
        return new MqttZeroRouteTrieNode(
            Endpoint,
            SingleLevel,
            multiLevel,
            _exactChildren);
    }

    /// <summary>
    /// 尝试GetExactChild。
    /// </summary>
    /// <param name="segment">段。</param>
    /// <param name="node">节点。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool TryGetExactChild(
        ReadOnlySpan<byte> segment,
        [NotNullWhen(true)] out MqttZeroRouteTrieNode? node)
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

    /// <summary>
    /// 设置ExactChild。
    /// </summary>
    /// <param name="segment">段。</param>
    /// <param name="node">节点。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 移除ExactChild。
    /// </summary>
    /// <param name="segment">段。</param>
    /// <returns>对应的结果。</returns>
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
