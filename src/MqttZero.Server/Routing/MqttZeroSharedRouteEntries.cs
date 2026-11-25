using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSharedRouteEntries
{
    private readonly MqttZeroPersistentHashMap<
        MqttZeroSessionState,
        MqttZeroSubscription> _entries =
        MqttZeroPersistentHashMap<
            MqttZeroSessionState,
            MqttZeroSubscription>.Create(
            EqualityComparer<MqttZeroSessionState>.Default);

    public MqttZeroSharedRouteEntries(MqttZeroSharedGroupKey key)
        : this(
            key,
            MqttZeroPersistentHashMap<
                MqttZeroSessionState,
                MqttZeroSubscription>.Create(
                EqualityComparer<MqttZeroSessionState>.Default))
    {
    }

    private MqttZeroSharedRouteEntries(
        MqttZeroSharedGroupKey key,
        MqttZeroPersistentHashMap<
            MqttZeroSessionState,
            MqttZeroSubscription> entries)
    {
        Key = key;
        _entries = entries;
    }

    public MqttZeroSharedGroupKey Key { get; }

    public int Count
    {
        get
        {
            return _entries.Count;
        }
    }

    internal MqttZeroPersistentHashMap<
        MqttZeroSessionState,
        MqttZeroSubscription> Entries
    {
        get
        {
            return _entries;
        }
    }

    public MqttZeroSharedRouteEntries Set(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        return new MqttZeroSharedRouteEntries(
            Key,
            _entries.Set(session, subscription, out _));
    }

    public MqttZeroSharedRouteEntries? Remove(
        MqttZeroSessionState session,
        out bool removed)
    {
        MqttZeroPersistentHashMap<
            MqttZeroSessionState,
            MqttZeroSubscription> entries = _entries.Remove(
                session,
                out removed);
        return removed && entries.Count > 0
            ? new MqttZeroSharedRouteEntries(Key, entries)
            : null;
    }
}
