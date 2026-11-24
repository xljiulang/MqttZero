namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSharedRouteGroup
{
    private int _cursor;

    public MqttZeroSharedRouteGroup(
        string group,
        MqttZeroSharedRouteEntry[] entries)
    {
        Group = group;
        Entries = entries;
    }

    public string Group { get; }

    public MqttZeroSharedRouteEntry[] Entries { get; }

    public int NextOffset(int count)
    {
        int value = Interlocked.Increment(ref _cursor) - 1;
        return (int)((uint)value % (uint)count);
    }
}
