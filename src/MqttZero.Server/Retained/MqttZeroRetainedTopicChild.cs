using MqttZero.Server.Routing;

namespace MqttZero.Server.Retained;

internal readonly struct MqttZeroRetainedTopicChild
{
    public MqttZeroRetainedTopicChild(
        MqttZeroTopicSegment segment,
        MqttZeroRetainedTopicNode node)
    {
        Segment = segment;
        Node = node;
    }

    public MqttZeroTopicSegment Segment { get; }

    public MqttZeroRetainedTopicNode Node { get; }
}
