namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSharedGroupCursor
{
    private int _value;

    internal int Value
    {
        get
        {
            return Volatile.Read(ref _value);
        }
    }

    public int NextOffset(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        int value = Interlocked.Increment(ref _value) - 1;
        return (int)((uint)value % (uint)count);
    }
}
