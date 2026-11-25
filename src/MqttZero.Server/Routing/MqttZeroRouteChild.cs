namespace MqttZero.Server.Routing;

internal readonly struct MqttZeroRouteChild
{
    public MqttZeroRouteChild(
        MqttZeroTopicSegment segment,
        MqttZeroRouteTrieNode node)
    {
        Segment = segment;
        Node = node;
    }

    public MqttZeroTopicSegment Segment { get; }

    public MqttZeroRouteTrieNode Node { get; }
}
