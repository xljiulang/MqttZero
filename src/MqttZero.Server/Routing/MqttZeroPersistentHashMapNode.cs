namespace MqttZero.Server.Routing;

internal abstract class MqttZeroPersistentHashMapNode<TKey, TValue>
{
    public abstract int Count { get; }

    public abstract bool TryGetValue(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        out TValue value);

    public abstract MqttZeroPersistentHashMapNode<TKey, TValue> Set(
        TKey key,
        TValue value,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        ref bool added);

    public abstract MqttZeroPersistentHashMapNode<TKey, TValue>? Remove(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        ref bool removed);
}
