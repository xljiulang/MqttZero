namespace MqttZero.Server.Routing;

internal readonly struct MqttZeroSharedGroupKey :
    IEquatable<MqttZeroSharedGroupKey>
{
    private readonly byte[] _group;
    private readonly byte[] _filter;

    public MqttZeroSharedGroupKey(
        ReadOnlySpan<byte> group,
        ReadOnlySpan<byte> filter)
    {
        _group = group.ToArray();
        _filter = filter.ToArray();
    }

    public ReadOnlySpan<byte> Group
    {
        get
        {
            return _group;
        }
    }

    public ReadOnlySpan<byte> Filter
    {
        get
        {
            return _filter;
        }
    }

    public bool Equals(MqttZeroSharedGroupKey other)
    {
        return Group.SequenceEqual(other.Group) &&
            Filter.SequenceEqual(other.Filter);
    }

    public override bool Equals(object? obj)
    {
        return obj is MqttZeroSharedGroupKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        uint hash = 2166136261;
        for (int index = 0; index < _group.Length; index++)
        {
            hash = (hash ^ _group[index]) * 16777619;
        }

        hash = (hash ^ 0xFF) * 16777619;
        for (int index = 0; index < _filter.Length; index++)
        {
            hash = (hash ^ _filter[index]) * 16777619;
        }

        return (int)hash;
    }
}
