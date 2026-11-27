using System.Numerics;

namespace MqttZero.Server.Routing;

/// <summary>
/// 持久化哈希映射的位图分支节点。
/// </summary>
internal sealed class MqttZeroPersistentHashMapBitmapNode<TKey, TValue> :
    MqttZeroPersistentHashMapNode<TKey, TValue>
{
    private readonly int _bitmap;
    private readonly MqttZeroPersistentHashMapNode<TKey, TValue>[] _children;
    private readonly int _count;

    /// <summary>
    /// 初始化 MqttZeroPersistentHashMapBitmapNode 的新实例。
    /// </summary>
    /// <param name="bitmap">对应的 bitmap。</param>
    /// <param name="children">对应的 children。</param>
    /// <param name="count">数量。</param>
    public MqttZeroPersistentHashMapBitmapNode(
        int bitmap,
        MqttZeroPersistentHashMapNode<TKey, TValue>[] children,
        int count)
    {
        _bitmap = bitmap;
        _children = children;
        _count = count;
    }

    public override int Count
    {
        get
        {
            return _count;
        }
    }

    internal ReadOnlySpan<MqttZeroPersistentHashMapNode<TKey, TValue>>
        Children
    {
        get
        {
            return _children;
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
        out TValue value)
    {
        int bit = GetBit(hash, shift);
        if ((_bitmap & bit) == 0)
        {
            value = default!;
            return false;
        }

        int index = GetChildIndex(bit);
        return _children[index].TryGetValue(
            key,
            hash,
            shift + 5,
            comparer,
            out value);
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
        int bit = GetBit(hash, shift);
        int index = GetChildIndex(bit);
        if ((_bitmap & bit) == 0)
        {
            MqttZeroPersistentHashMapNode<TKey, TValue>[] children =
                new MqttZeroPersistentHashMapNode<TKey, TValue>[
                    _children.Length + 1];
            _children.AsSpan(0, index).CopyTo(children);
            children[index] =
                new MqttZeroPersistentHashMapLeafNode<TKey, TValue>(
                    key,
                    value,
                    hash);
            _children.AsSpan(index).CopyTo(children.AsSpan(index + 1));
            added = true;
            return new MqttZeroPersistentHashMapBitmapNode<TKey, TValue>(
                _bitmap | bit,
                children,
                _count + 1);
        }

        MqttZeroPersistentHashMapNode<TKey, TValue> child =
            _children[index];
        MqttZeroPersistentHashMapNode<TKey, TValue> replacement =
            child.Set(
                key,
                value,
                hash,
                shift + 5,
                comparer,
                ref added);
        if (ReferenceEquals(child, replacement))
        {
            return this;
        }

        MqttZeroPersistentHashMapNode<TKey, TValue>[] replacementChildren =
            new MqttZeroPersistentHashMapNode<TKey, TValue>[_children.Length];
        _children.CopyTo(replacementChildren, 0);
        replacementChildren[index] = replacement;
        return new MqttZeroPersistentHashMapBitmapNode<TKey, TValue>(
            _bitmap,
            replacementChildren,
            _count + (replacement.Count - child.Count));
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
        int bit = GetBit(hash, shift);
        if ((_bitmap & bit) == 0)
        {
            return this;
        }

        int index = GetChildIndex(bit);
        MqttZeroPersistentHashMapNode<TKey, TValue> child = _children[index];
        MqttZeroPersistentHashMapNode<TKey, TValue>? replacement =
            child.Remove(
                key,
                hash,
                shift + 5,
                comparer,
                ref removed);
        if (!removed)
        {
            return this;
        }

        if (replacement == null)
        {
            if (_children.Length == 1)
            {
                return null;
            }

            MqttZeroPersistentHashMapNode<TKey, TValue>[] children =
                new MqttZeroPersistentHashMapNode<TKey, TValue>[
                    _children.Length - 1];
            _children.AsSpan(0, index).CopyTo(children);
            _children.AsSpan(index + 1).CopyTo(
                children.AsSpan(index));
            return new MqttZeroPersistentHashMapBitmapNode<TKey, TValue>(
                _bitmap & ~bit,
                children,
                _count - child.Count);
        }

        if (ReferenceEquals(child, replacement))
        {
            return this;
        }

        MqttZeroPersistentHashMapNode<TKey, TValue>[] replacementChildren =
            new MqttZeroPersistentHashMapNode<TKey, TValue>[_children.Length];
        _children.CopyTo(replacementChildren, 0);
        replacementChildren[index] = replacement;
        return new MqttZeroPersistentHashMapBitmapNode<TKey, TValue>(
            _bitmap,
            replacementChildren,
            _count + (replacement.Count - child.Count));
    }

    /// <summary>
    /// 获取Child索引。
    /// </summary>
    /// <param name="bit">位。</param>
    /// <returns>计算得到的数值。</returns>
    private int GetChildIndex(int bit)
    {
        return BitOperations.PopCount(
            (uint)(_bitmap & (bit - 1)));
    }

    /// <summary>
    /// 获取位。
    /// </summary>
    /// <param name="hash">对应的 hash。</param>
    /// <param name="shift">对应的 shift。</param>
    /// <returns>计算得到的数值。</returns>
    private static int GetBit(int hash, uint shift)
    {
        return 1 << (int)((uint)hash >> (int)shift & 31U);
    }
}
