using System.Collections.Concurrent;

namespace MqttZero.Memory;

/// <summary>
/// 有容量上限的对象池。
/// </summary>
internal sealed class MqttZeroBoundedObjectPool<T>
    where T : class
{
    /// <summary>
    /// 默认容量。
    /// </summary>
    public static int DefaultCapacity { get; } = Math.Clamp(
        Environment.ProcessorCount * 256,
        1_024,
        4_096);

    private readonly ConcurrentQueue<T> _items = new();

    /// <summary>
    /// 初始化 MqttZeroBoundedObjectPool 的新实例。
    /// </summary>
    /// <param name="capacity">容量。</param>
    public MqttZeroBoundedObjectPool(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Capacity = capacity;
    }

    /// <summary>
    /// 容量。
    /// </summary>
    public int Capacity { get; }

    /// <summary>
    /// Count 的值。
    /// </summary>
    public int Count
    {
        get
        {
            return _items.Count;
        }
    }

    /// <summary>
    /// 尝试租用。
    /// </summary>
    /// <param name="item">项。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool TryRent(out T? item)
    {
        if (_items.TryDequeue(out item))
        {
            return true;
        }

        item = null;
        return false;
    }

    /// <summary>
    /// 归还。
    /// </summary>
    /// <param name="item">项。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool Return(T item)
    {
        ArgumentNullException.ThrowIfNull(item);
        if (_items.Count < Capacity)
        {
            _items.Enqueue(item);
            return true;
        }

        return false;
    }
}
