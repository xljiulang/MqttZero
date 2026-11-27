using System.Diagnostics.CodeAnalysis;
namespace MqttZero.Server.Routing;

/// <summary>
/// 持久化哈希映射的哈希冲突叶子集合。
/// </summary>
internal sealed class MqttZeroPersistentHashMapCollisionNode<TKey, TValue> :
    MqttZeroPersistentHashMapNode<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    private readonly MqttZeroPersistentHashMapEntry<TKey, TValue>[] _entries;
    private readonly int _hash;

    /// <summary>
    /// 初始化 MqttZeroPersistentHashMapCollisionNode 的新实例。
    /// </summary>
    /// <param name="hash">对应的 hash。</param>
    /// <param name="entries">条目。</param>
    public MqttZeroPersistentHashMapCollisionNode(
        int hash,
        MqttZeroPersistentHashMapEntry<TKey, TValue>[] entries)
    {
        _hash = hash;
        _entries = entries;
    }

    /// <summary>
    /// Count 的值。
    /// </summary>
    public override int Count
    {
        get
        {
            return _entries.Length;
        }
    }

    /// <summary>
    /// 条目。
    /// </summary>
    internal ReadOnlySpan<MqttZeroPersistentHashMapEntry<TKey, TValue>>
        Entries
    {
        get
        {
            return _entries;
        }
    }

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
        [MaybeNullWhen(false)] out TValue value)
    {
        if (_hash == hash)
        {
            for (int index = 0; index < _entries.Length; index++)
            {
                MqttZeroPersistentHashMapEntry<TKey, TValue> entry =
                    _entries[index];
                if (comparer.Equals(entry.Key, key))
                {
                    value = entry.Value;
                    return true;
                }
            }
        }

        value = default;
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
        if (hash != _hash)
        {
            throw new InvalidOperationException(
                "A collision node received a different hash code.");
        }

        for (int index = 0; index < _entries.Length; index++)
        {
            MqttZeroPersistentHashMapEntry<TKey, TValue> entry =
                _entries[index];
            if (comparer.Equals(entry.Key, key))
            {
                MqttZeroPersistentHashMapEntry<TKey, TValue>[] replacement =
                    new MqttZeroPersistentHashMapEntry<TKey, TValue>[
                        _entries.Length];
                _entries.AsSpan(0, index).CopyTo(replacement);
                replacement[index] = new MqttZeroPersistentHashMapEntry<
                    TKey,
                    TValue>(key, value);
                _entries.AsSpan(index + 1).CopyTo(
                    replacement.AsSpan(index + 1));

                added = false;
                return new MqttZeroPersistentHashMapCollisionNode<TKey, TValue>(
                    _hash,
                    replacement);
            }
        }

        MqttZeroPersistentHashMapEntry<TKey, TValue>[] appended =
            new MqttZeroPersistentHashMapEntry<TKey, TValue>[
                _entries.Length + 1];
        _entries.CopyTo(appended, 0);
        appended[^1] = new MqttZeroPersistentHashMapEntry<TKey, TValue>(
            key,
            value);
        added = true;
        return new MqttZeroPersistentHashMapCollisionNode<TKey, TValue>(
            _hash,
            appended);
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
        if (hash != _hash)
        {
            return this;
        }

        int removedIndex = -1;
        for (int index = 0; index < _entries.Length; index++)
        {
            if (comparer.Equals(_entries[index].Key, key))
            {
                removedIndex = index;
                break;
            }
        }

        if (removedIndex < 0)
        {
            return this;
        }

        removed = true;
        if (_entries.Length == 1)
        {
            return null;
        }

        MqttZeroPersistentHashMapEntry<TKey, TValue>[] replacement =
            new MqttZeroPersistentHashMapEntry<TKey, TValue>[
                _entries.Length - 1];
        _entries.AsSpan(0, removedIndex).CopyTo(replacement);
        _entries.AsSpan(removedIndex + 1).CopyTo(
            replacement.AsSpan(removedIndex));
        return new MqttZeroPersistentHashMapCollisionNode<TKey, TValue>(
            _hash,
            replacement);
    }
}
