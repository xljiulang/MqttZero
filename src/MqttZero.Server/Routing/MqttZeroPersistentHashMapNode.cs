using System.Diagnostics.CodeAnalysis;
namespace MqttZero.Server.Routing;

/// <summary>
/// 持久化哈希映射的节点抽象。
/// </summary>
internal abstract class MqttZeroPersistentHashMapNode<TKey, TValue>
    where TKey : notnull
    where TValue : notnull
{
    /// <summary>
    /// 数量。
    /// </summary>
    public abstract int Count { get; }

    /// <summary>
    /// 尝试Get值。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="hash">对应的 hash。</param>
    /// <param name="shift">对应的 shift。</param>
    /// <param name="comparer">对应的 comparer。</param>
    /// <param name="value">值。</param>
    /// <returns>对应的结果。</returns>
    public abstract bool TryGetValue(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        [MaybeNullWhen(false)] out TValue value);

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
    public abstract MqttZeroPersistentHashMapNode<TKey, TValue> Set(
        TKey key,
        TValue value,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        ref bool added);

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="hash">对应的 hash。</param>
    /// <param name="shift">对应的 shift。</param>
    /// <param name="comparer">对应的 comparer。</param>
    /// <param name="removed">对应的 removed。</param>
    /// <returns>对应的结果。</returns>
    public abstract MqttZeroPersistentHashMapNode<TKey, TValue>? Remove(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        ref bool removed);
}
