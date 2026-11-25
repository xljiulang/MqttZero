namespace MqttZero.Server.Routing;

internal readonly struct MqttZeroPersistentHashMapEntry<TKey, TValue>
{
    public MqttZeroPersistentHashMapEntry(TKey key, TValue value)
    {
        Key = key;
        Value = value;
    }

    public TKey Key { get; }

    public TValue Value { get; }
}
