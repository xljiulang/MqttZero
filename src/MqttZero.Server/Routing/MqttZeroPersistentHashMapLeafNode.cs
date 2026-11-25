namespace MqttZero.Server.Routing;

internal sealed class MqttZeroPersistentHashMapLeafNode<TKey, TValue> :
    MqttZeroPersistentHashMapNode<TKey, TValue>
{
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

    internal TKey Key { get; }

    internal TValue Value { get; }

    internal int Hash { get; }

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
