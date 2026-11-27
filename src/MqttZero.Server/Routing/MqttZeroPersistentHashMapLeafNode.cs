namespace MqttZero.Server.Routing;

/// <summary>
/// 持久化哈希映射的单个键值叶子。
/// </summary>
internal sealed class MqttZeroPersistentHashMapLeafNode<TKey, TValue> :
    MqttZeroPersistentHashMapNode<TKey, TValue>
{
    /// <summary>
    /// 初始化 MqttZeroPersistentHashMapLeafNode 的新实例。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <param name="hash">对应的 hash。</param>
    public MqttZeroPersistentHashMapLeafNode(
        TKey key,
        TValue value,
        int hash)
    {
        Key = key;
        Value = value;
        Hash = hash;
    }

    public override int Count
    {
        get
        {
            return 1;
        }
    }

    /// <summary>
    /// 键。
    /// </summary>
    internal TKey Key { get; }

    /// <summary>
    /// 值。
    /// </summary>
    internal TValue Value { get; }

    /// <summary>
    /// Hash 的值。
    /// </summary>
    internal int Hash { get; }

    /// <summary>
    /// 尝试Get值。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="hash">对应的 hash。</param>
    /// <param name="shift">对应的 shift。</param>
    /// <param name="comparer">对应的 comparer。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public override bool TryGetValue(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        out TValue value)
    {
        if (Hash == hash && comparer.Equals(Key, key))
        {
            value = Value;
            return true;
        }

        value = default!;
        return false;
    }

    /// <summary>
    /// 设置。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <param name="hash">对应的 hash。</param>
    /// <param name="shift">对应的 shift。</param>
    /// <param name="comparer">对应的 comparer。</param>
    /// <param name="added">对应的 added。</param>
    /// <returns>对应的结果。</returns>
    public override MqttZeroPersistentHashMapNode<TKey, TValue> Set(
        TKey key,
        TValue value,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        ref bool added)
    {
        if (Hash == hash && comparer.Equals(Key, key))
        {
            added = false;
            return new MqttZeroPersistentHashMapLeafNode<TKey, TValue>(
                key,
                value,
                hash);
        }

        added = true;
        if (shift >= 32)
        {
            return new MqttZeroPersistentHashMapCollisionNode<TKey, TValue>(
                Hash,
                [
                    new MqttZeroPersistentHashMapEntry<TKey, TValue>(
                        Key,
                        Value),
                    new MqttZeroPersistentHashMapEntry<TKey, TValue>(
                        key,
                        value)
                ]);
        }

        MqttZeroPersistentHashMapBitmapNode<TKey, TValue> bitmap = new(
            0,
            [],
            0);
        bitmap = (MqttZeroPersistentHashMapBitmapNode<TKey, TValue>)bitmap.Set(
            Key,
            Value,
            Hash,
            shift,
            comparer,
            ref added);
        bitmap = (MqttZeroPersistentHashMapBitmapNode<TKey, TValue>)bitmap.Set(
            key,
            value,
            hash,
            shift,
            comparer,
            ref added);
        return bitmap;
    }

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="hash">对应的 hash。</param>
    /// <param name="shift">对应的 shift。</param>
    /// <param name="comparer">对应的 comparer。</param>
    /// <param name="removed">对应的 removed。</param>
    /// <returns>对应的结果。</returns>
    public override MqttZeroPersistentHashMapNode<TKey, TValue>? Remove(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        ref bool removed)
    {
        if (Hash != hash || !comparer.Equals(Key, key))
        {
            return this;
        }

        removed = true;
        return null;
    }
}
