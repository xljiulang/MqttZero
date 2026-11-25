namespace MqttZero.Server.Routing;

internal sealed class MqttZeroPersistentHashMapCollisionNode<TKey, TValue> :
    MqttZeroPersistentHashMapNode<TKey, TValue>
{
    private readonly MqttZeroPersistentHashMapEntry<TKey, TValue>[] _entries;
    private readonly int _hash;

    public MqttZeroPersistentHashMapCollisionNode(
        int hash,
        MqttZeroPersistentHashMapEntry<TKey, TValue>[] entries)
    {
        _hash = hash;
        _entries = entries;
    }

    public override int Count
    {
        get
        {
            return _entries.Length;
        }
    }

    internal ReadOnlySpan<MqttZeroPersistentHashMapEntry<TKey, TValue>>
        Entries
    {
        get
        {
            return _entries;
        }
    }

    public override bool TryGetValue(
        TKey key,
        int hash,
        uint shift,
        IEqualityComparer<TKey> comparer,
        out TValue value)
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
