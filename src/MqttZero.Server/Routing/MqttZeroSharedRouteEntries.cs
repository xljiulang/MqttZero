using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSharedRouteEntries
{
    private readonly SharedGroupCursor _cursor;
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
                EqualityComparer<MqttZeroSessionState>.Default),
            new SharedGroupCursor())
    {
    }

    private MqttZeroSharedRouteEntries(
        MqttZeroSharedGroupKey key,
        MqttZeroPersistentHashMap<
            MqttZeroSessionState,
            MqttZeroSubscription> entries,
        SharedGroupCursor cursor)
    {
        Key = key;
        _entries = entries;
        _cursor = cursor;
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

    public int NextOffset(int count)
    {
        return _cursor.NextOffset(count);
    }

    public MqttZeroSharedRouteEntries Set(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        return new MqttZeroSharedRouteEntries(
            Key,
            _entries.Set(session, subscription, out _),
            _cursor);
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
            ? new MqttZeroSharedRouteEntries(Key, entries, _cursor)
            : null;
    }

    private sealed class SharedGroupCursor
    {
        private int _value;

        public int NextOffset(int count)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
            int value = Interlocked.Increment(ref _value) - 1;
            return (int)((uint)value % (uint)count);
        }
    }
}
