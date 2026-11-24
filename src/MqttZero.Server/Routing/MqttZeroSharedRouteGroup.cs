namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSharedRouteGroup
{
    private int _cursor;

    public MqttZeroSharedRouteGroup(
        string group,
        MqttZeroSharedRouteEntry[] entries,
        string? cursorKey = null,
        int initialCursor = 0)
    {
        Group = group;
        Entries = entries;
        CursorKey = cursorKey ?? group;
        _cursor = initialCursor;
    }

    public string Group { get; }

    public MqttZeroSharedRouteEntry[] Entries { get; }

    public string CursorKey { get; }

    public int Cursor
    {
        get
        {
            return Volatile.Read(ref _cursor);
        }
    }

    public int NextOffset(int count)
    {
        int value = Interlocked.Increment(ref _cursor) - 1;
        return (int)((uint)value % (uint)count);
    }
}
