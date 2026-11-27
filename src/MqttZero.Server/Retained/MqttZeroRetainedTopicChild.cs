using MqttZero.Server.Routing;

namespace MqttZero.Server.Retained;

/// <summary>
/// 保留消息主题树的子节点。
/// </summary>
internal readonly struct MqttZeroRetainedTopicChild
{
    /// <summary>
    /// 初始化 MqttZeroRetainedTopicChild 的新实例。
    /// </summary>
    /// <param name="segment">段。</param>
    /// <param name="node">节点。</param>
    public MqttZeroRetainedTopicChild(
        MqttZeroTopicSegment segment,
        MqttZeroRetainedTopicNode node)
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
    public MqttZeroRetainedTopicNode Node { get; }
}
