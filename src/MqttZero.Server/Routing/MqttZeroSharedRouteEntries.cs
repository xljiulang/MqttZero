using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

/// <summary>
/// 共享订阅在路由节点上的条目集合。
/// </summary>
internal sealed class MqttZeroSharedRouteEntries
{
    private readonly MqttZeroPersistentHashMap<
        MqttZeroSessionState,
        MqttZeroSubscription> _entries =
        MqttZeroPersistentHashMap<
            MqttZeroSessionState,
            MqttZeroSubscription>.Create(
            EqualityComparer<MqttZeroSessionState>.Default);

    /// <summary>
    /// 初始化 MqttZeroSharedRouteEntries 的新实例。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="cursor">游标。</param>
    public MqttZeroSharedRouteEntries(
        MqttZeroSharedGroupKey key,
        MqttZeroSharedGroupCursor cursor)
        : this(
            key,
            cursor,
            MqttZeroPersistentHashMap<
                MqttZeroSessionState,
                MqttZeroSubscription>.Create(
                EqualityComparer<MqttZeroSessionState>.Default))
    {
    }

    /// <summary>
    /// 初始化 MqttZeroSharedRouteEntries 的新实例。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="cursor">游标。</param>
    /// <param name="entries">条目。</param>
    private MqttZeroSharedRouteEntries(
        MqttZeroSharedGroupKey key,
        MqttZeroSharedGroupCursor cursor,
        MqttZeroPersistentHashMap<
            MqttZeroSessionState,
            MqttZeroSubscription> entries)
    {
        Key = key;
        Cursor = cursor ??
            throw new ArgumentNullException(nameof(cursor));
        _entries = entries;
    }

    /// <summary>
    /// 键。
    /// </summary>
    public MqttZeroSharedGroupKey Key { get; }

    /// <summary>
    /// 游标。
    /// </summary>
    public MqttZeroSharedGroupCursor Cursor { get; }

    public int Count
    {
        get
        {
            return _entries.Count;
        }
    }

    internal MqttZeroPersistentHashMap<
        MqttZeroSessionState,
        MqttZeroSubscription> Entries
    {
        get
        {
            return _entries;
        }
    }

    /// <summary>
    /// 设置。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroSharedRouteEntries Set(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        return new MqttZeroSharedRouteEntries(
            Key,
            Cursor,
            _entries.Set(session, subscription, out _));
    }

    /// <summary>
    /// 移除。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="removed">对应的 removed。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroSharedRouteEntries? Remove(
        MqttZeroSessionState session,
        out bool removed)
    {
        MqttZeroPersistentHashMap<
            MqttZeroSessionState,
            MqttZeroSubscription> entries = _entries.Remove(
                session,
                out removed);
        return removed && entries.Count > 0
            ? new MqttZeroSharedRouteEntries(Key, Cursor, entries)
            : null;
    }
}
