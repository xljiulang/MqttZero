namespace MqttZero.Server.Routing;

internal sealed class MqttZeroTopicSegmentComparer :
    IComparer<MqttZeroTopicSegment>
{
    public static MqttZeroTopicSegmentComparer Instance { get; } = new();

    public int Compare(
        MqttZeroTopicSegment x,
        MqttZeroTopicSegment y)
    {
        return Compare(x.Span, y.Span);
    }

    public static int Compare(
        ReadOnlySpan<byte> x,
        ReadOnlySpan<byte> y)
    {
        return x.SequenceCompareTo(y);
    }
}
