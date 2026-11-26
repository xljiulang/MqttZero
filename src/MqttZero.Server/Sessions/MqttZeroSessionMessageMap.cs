using System.Collections;

namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroSessionMessageMap<T> :
    IEnumerable<KeyValuePair<ushort, T>>
{
    private readonly Dictionary<ushort, T> _items = [];
    private int _count;

    public int Count
    {
        get
        {
            return Volatile.Read(ref _count);
        }
    }

    public IEnumerable<T> Values
    {
        get
        {
            return _items.Values;
        }
    }

    public T this[ushort key]
    {
        get
        {
            return _items[key];
        }
        set
        {
            if (!_items.ContainsKey(key))
            {
                _items[key] = value;
                Interlocked.Increment(ref _count);
                return;
            }

            _items[key] = value;
        }
    }

    public void Add(ushort key, T value)
    {
        _items.Add(key, value);
        Interlocked.Increment(ref _count);
    }

    public bool ContainsKey(ushort key)
    {
        return _items.ContainsKey(key);
    }

    public bool TryGetValue(ushort key, out T value)
    {
        return _items.TryGetValue(key, out value!);
    }

    public bool Remove(ushort key)
    {
        if (!_items.Remove(key))
        {
            return false;
        }

        Interlocked.Decrement(ref _count);
        return true;
    }

    public bool Remove(ushort key, out T value)
    {
        if (!_items.Remove(key, out value!))
        {
            return false;
        }

        Interlocked.Decrement(ref _count);
        return true;
    }

    public void Clear()
    {
        _items.Clear();
        Volatile.Write(ref _count, 0);
    }

    public IEnumerator<KeyValuePair<ushort, T>> GetEnumerator()
    {
        return _items.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
