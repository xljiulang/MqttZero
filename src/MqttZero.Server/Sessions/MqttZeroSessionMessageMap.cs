using System.Diagnostics.CodeAnalysis;
using System.Collections;

namespace MqttZero.Server.Sessions;

/// <summary>
/// 会话消息标识到消息的映射。
/// </summary>
internal sealed class MqttZeroSessionMessageMap<T> :
    IEnumerable<KeyValuePair<ushort, T>>
    where T : notnull
{
    private readonly Dictionary<ushort, T> _items = [];
    private int _count;

    /// <summary>
    /// Count 的值。
    /// </summary>
    public int Count
    {
        get
        {
            return Volatile.Read(ref _count);
        }
    }

    /// <summary>
    /// Values 的值。
    /// </summary>
    public IEnumerable<T> Values
    {
        get
        {
            return _items.Values;
        }
    }

    /// <summary>
    /// 按索引获取元素。
    /// </summary>
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

    /// <summary>
    /// 添加。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    public void Add(ushort key, T value)
    {
        _items.Add(key, value);
        Interlocked.Increment(ref _count);
    }

    /// <summary>
    /// 尝试添加：键已存在时不覆盖并返回 <see langword="false"/>。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool TryAdd(ushort key, T value)
    {
        if (!_items.TryAdd(key, value))
        {
            return false;
        }

        Interlocked.Increment(ref _count);
        return true;
    }

    /// <summary>
    /// Contains键。
    /// </summary>
    /// <param name="key">键。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool ContainsKey(ushort key)
    {
        return _items.ContainsKey(key);
    }

    /// <summary>
    /// 尝试Get值。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool TryGetValue(ushort key, [MaybeNullWhen(false)] out T value)
    {
        return _items.TryGetValue(key, out value);
    }

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="key">键。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool Remove(ushort key)
    {
        if (!_items.Remove(key))
        {
            return false;
        }

        Interlocked.Decrement(ref _count);
        return true;
    }

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool Remove(ushort key, [MaybeNullWhen(false)] out T value)
    {
        if (!_items.Remove(key, out value))
        {
            return false;
        }

        Interlocked.Decrement(ref _count);
        return true;
    }

    /// <summary>
    /// 清空。
    /// </summary>
    public void Clear()
    {
        _items.Clear();
        Volatile.Write(ref _count, 0);
    }

    /// <summary>
    /// 获取Enumerator。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public IEnumerator<KeyValuePair<ushort, T>> GetEnumerator()
    {
        return _items.GetEnumerator();
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
