namespace MqttZero.Server.Routing;

/// <summary>
/// 前缀树的子节点条目。
/// </summary>
internal readonly struct MqttZeroRouteChild
{
    /// <summary>
    /// 初始化 MqttZeroRouteChild 的新实例。
    /// </summary>
    /// <param name="segment">段。</param>
    /// <param name="node">节点。</param>
    public MqttZeroRouteChild(
        MqttZeroTopicSegment segment,
        MqttZeroRouteTrieNode node)
    {
        Segment = segment;
        Node = node;
    }

    /// <summary>
    /// 段。
    /// </summary>
    public MqttZeroTopicSegment Segment { get; }

    /// <summary>
    /// 节点。
    /// </summary>
    public MqttZeroRouteTrieNode Node { get; }
}
