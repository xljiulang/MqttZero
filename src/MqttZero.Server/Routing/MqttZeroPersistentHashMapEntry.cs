namespace MqttZero.Server.Routing;

/// <summary>
/// 持久化哈希映射的键值条目。
/// </summary>
internal readonly struct MqttZeroPersistentHashMapEntry<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    /// <summary>
    /// 初始化 MqttZeroPersistentHashMapEntry 的新实例。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    public MqttZeroPersistentHashMapEntry(TKey key, TValue value)
    {
        Key = key;
        Value = value;
    }

    /// <summary>
    /// 键。
    /// </summary>
    public TKey Key { get; }

    /// <summary>
    /// 值。
    /// </summary>
    public TValue Value { get; }
}
