using System.Collections.Concurrent;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSharedGroupCursors
{
    private readonly ConcurrentDictionary<
        MqttZeroSharedGroupKey,
        MqttZeroSharedGroupCursor> _cursors = new();

    public int Count
    {
        get
        {
            return _cursors.Count;
        }
    }

    public int GetNext(MqttZeroSharedGroupKey key, int count)
    {
        return GetOrAddCursor(key).NextOffset(count);
    }

    public MqttZeroSharedGroupCursor Resolve(MqttZeroSharedGroupKey key)
    {
        return GetOrAddCursor(key);
    }

    public int GetCursor(MqttZeroSharedGroupKey key)
    {
        return GetOrAddCursor(key).Value;
    }

    private MqttZeroSharedGroupCursor GetOrAddCursor(
        MqttZeroSharedGroupKey key)
    {
        while (true)
        {
            if (_cursors.TryGetValue(
                key,
                out MqttZeroSharedGroupCursor? cursor))
            {
                return cursor;
            }

            MqttZeroSharedGroupCursor candidate = new();
            if (_cursors.TryAdd(key, candidate))
            {
                return candidate;
            }
        }
    }

    public void PruneTo(
        IReadOnlyCollection<MqttZeroSharedGroupKey> activeKeys)
    {
        ArgumentNullException.ThrowIfNull(activeKeys);
        HashSet<MqttZeroSharedGroupKey> active = new(activeKeys);
        foreach (MqttZeroSharedGroupKey key in _cursors.Keys)
        {
            if (!active.Contains(key))
            {
                _cursors.TryRemove(key, out _);
            }
        }
    }

    public void Remove(MqttZeroSharedGroupKey key)
    {
        _cursors.TryRemove(key, out _);
    }
}
