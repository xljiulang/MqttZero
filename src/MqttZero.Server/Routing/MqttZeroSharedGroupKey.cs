using MqttZero.Server.Sessions;
using System.Text;

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

    /// <summary>
    /// Creates the key of one shared subscription.
    /// </summary>
    /// <param name="subscription">The shared subscription.</param>
    /// <returns>The shared group key.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when the subscription is not a shared subscription.
    /// </exception>
    public static MqttZeroSharedGroupKey Create(
        MqttZeroSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        if (!subscription.TryGetSharedGroup(out string? sharedGroup))
        {
            throw new ArgumentException(
                "The subscription is not a shared subscription.",
                nameof(subscription));
        }

        return new MqttZeroSharedGroupKey(
            Encoding.UTF8.GetBytes(sharedGroup),
            subscription.MatchFilter);
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
