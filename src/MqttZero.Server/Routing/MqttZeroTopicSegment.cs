namespace MqttZero.Server.Routing;

internal readonly struct MqttZeroTopicSegment : IEquatable<MqttZeroTopicSegment>
{
    private readonly byte[] _bytes;

    private MqttZeroTopicSegment(byte[] bytes)
    {
        _bytes = bytes;
    }

    public ReadOnlySpan<byte> Span
    {
        get
        {
            return _bytes;
        }
    }

    public static MqttZeroTopicSegment From(ReadOnlySpan<byte> value)
    {
        return new MqttZeroTopicSegment(value.ToArray());
    }

    public bool Equals(MqttZeroTopicSegment other)
    {
        return Span.SequenceEqual(other.Span);
    }

    public override bool Equals(object? obj)
    {
        return obj is MqttZeroTopicSegment other && Equals(other);
    }

    public override int GetHashCode()
    {
        uint hash = 2166136261;
        for (int index = 0; index < _bytes.Length; index++)
        {
            hash = (hash ^ _bytes[index]) * 16777619;
        }

        return (int)hash;
    }
}
