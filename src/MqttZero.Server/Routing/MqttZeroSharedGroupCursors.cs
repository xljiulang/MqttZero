using System.Collections.Concurrent;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSharedGroupCursors
{
    private readonly ConcurrentDictionary<
        MqttZeroSharedGroupKey,
        MqttZeroSharedGroupCursor> _cursors = new();

    public int GetNext(MqttZeroSharedGroupKey key, int count)
    {
        return GetOrAddCursor(key).NextOffset(count);
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
}
