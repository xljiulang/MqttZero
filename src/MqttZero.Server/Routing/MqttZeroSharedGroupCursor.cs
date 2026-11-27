namespace MqttZero.Server.Routing;

/// <summary>
/// 单个共享组的轮询游标。
/// </summary>
internal sealed class MqttZeroSharedGroupCursor
{
    private int _value;

    /// <summary>
    /// Value 的值。
    /// </summary>
    internal int Value
    {
        get
        {
            return Volatile.Read(ref _value);
        }
    }

    /// <summary>
    /// 下一个偏移。
    /// </summary>
    /// <param name="count">数量。</param>
    /// <returns>计算得到的数值。</returns>
    public int NextOffset(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        int value = Interlocked.Increment(ref _value) - 1;
        return (int)((uint)value % (uint)count);
    }
}
