using System.Collections.Concurrent;

namespace MqttZero.Server.Routing;

/// <summary>
/// 共享组游标的注册表。
/// </summary>
internal sealed class MqttZeroSharedGroupCursors
{
    private readonly ConcurrentDictionary<MqttZeroSharedGroupKey, MqttZeroSharedGroupCursor> _cursors = new();

    /// <summary>
    /// Count 的值。
    /// </summary>
    public int Count
    {
        get
        {
            return _cursors.Count;
        }
    }

    /// <summary>
    /// 获取或创建某个共享组的游标实例。
    /// </summary>
    /// <param name="key">The shared group key.</param>
    /// <returns>The cursor owned by the shared group.</returns>
    public MqttZeroSharedGroupCursor Resolve(MqttZeroSharedGroupKey key)
    {
        return GetOrAddCursor(key);
    }

    /// <summary>
    /// 获取OrAdd游标。
    /// </summary>
    /// <param name="key">键。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroSharedGroupCursor GetOrAddCursor(
        MqttZeroSharedGroupKey key)
    {
        while (true)
        {
            if (_cursors.TryGetValue(
                key,
                out MqttZeroSharedGroupCursor? cursor))
            {
                return cursor;
            }

            MqttZeroSharedGroupCursor candidate = new();
            if (_cursors.TryAdd(key, candidate))
            {
                return candidate;
            }
        }
    }

    /// <summary>
    /// 修剪To。
    /// </summary>
    /// <param name="activeKeys">active键。</param>
    public void PruneTo(
        IReadOnlyCollection<MqttZeroSharedGroupKey> activeKeys)
    {
        ArgumentNullException.ThrowIfNull(activeKeys);
        HashSet<MqttZeroSharedGroupKey> active = new(activeKeys);
        foreach (MqttZeroSharedGroupKey key in _cursors.Keys)
        {
            if (!active.Contains(key))
            {
                _cursors.TryRemove(key, out _);
            }
        }
    }

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="key">键。</param>
    public void Remove(MqttZeroSharedGroupKey key)
    {
        _cursors.TryRemove(key, out _);
    }
}
