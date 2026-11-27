using MqttZero.Server.Sessions;
using RegularRouteMap =
    MqttZero.Server.Routing.MqttZeroPersistentHashMap<
        MqttZero.Server.Sessions.MqttZeroSessionState,
        MqttZero.Server.Sessions.MqttZeroSubscription>;
using SharedRouteMap =
    MqttZero.Server.Routing.MqttZeroPersistentHashMap<
        MqttZero.Server.Routing.MqttZeroSharedGroupKey,
        MqttZero.Server.Routing.MqttZeroSharedRouteEntries>;

namespace MqttZero.Server.Routing;

/// <summary>
/// 前缀树叶子上的订阅集合。
/// </summary>
internal sealed class MqttZeroRouteEndpoint
{
    /// <summary>
    /// 空。
    /// </summary>
    public static MqttZeroRouteEndpoint Empty { get; } = new(null, null);

    /// <summary>
    /// 初始化 MqttZeroRouteEndpoint 的新实例。
    /// </summary>
    /// <param name="regularEntries">regular条目。</param>
    /// <param name="sharedEntries">共享条目。</param>
    private MqttZeroRouteEndpoint(
        RegularRouteMap? regularEntries,
        SharedRouteMap? sharedEntries)
    {
        RegularEntries = regularEntries;
        SharedEntries = sharedEntries;
    }

    /// <summary>
    /// Regular条目。
    /// </summary>
    public RegularRouteMap? RegularEntries { get; }

    /// <summary>
    /// 共享条目。
    /// </summary>
    public SharedRouteMap? SharedEntries { get; }

    /// <summary>
    /// IsEmpty 的值。
    /// </summary>
    public bool IsEmpty
    {
        get
        {
            return (RegularEntries == null || RegularEntries.Count == 0) &&
                (SharedEntries == null || SharedEntries.Count == 0);
        }
    }

    /// <summary>
    /// Collect共享组键。
    /// </summary>
    /// <param name="keys">键。</param>
    public void CollectSharedGroupKeys(
        HashSet<MqttZeroSharedGroupKey> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (SharedEntries == null)
        {
            return;
        }

        foreach (MqttZeroPersistentHashMapEntry<
            MqttZeroSharedGroupKey,
            MqttZeroSharedRouteEntries> entry in SharedEntries)
        {
            keys.Add(entry.Key);
        }
    }

    /// <summary>
    /// 设置Regular。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteEndpoint SetRegular(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        RegularRouteMap entries = RegularEntries ??
            RegularRouteMap.Create(
                EqualityComparer<MqttZeroSessionState>.Default);
        return new MqttZeroRouteEndpoint(
            entries.Set(session, subscription, out _),
            SharedEntries);
    }

    /// <summary>
    /// 移除Regular。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="removed">对应的 removed。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteEndpoint? RemoveRegular(
        MqttZeroSessionState session,
        out bool removed)
    {
        if (RegularEntries == null)
        {
            removed = false;
            return this;
        }

        RegularRouteMap entries = RegularEntries.Remove(
                session,
                out removed);
        if (!removed)
        {
            return this;
        }

        MqttZeroRouteEndpoint result = new(
            entries.Count > 0 ? entries : null,
            SharedEntries);
        return result.IsEmpty ? null : result;
    }

    /// <summary>
    /// 设置共享。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="session">会话。</param>
    /// <param name="subscription">订阅。</param>
    /// <param name="cursor">游标。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteEndpoint SetShared(
        MqttZeroSharedGroupKey key,
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        MqttZeroSharedGroupCursor cursor)
    {
        ArgumentNullException.ThrowIfNull(cursor);
        SharedRouteMap entries = SharedEntries ??
            SharedRouteMap.Create(
                EqualityComparer<MqttZeroSharedGroupKey>.Default);
        MqttZeroSharedRouteEntries routeEntries =
            entries.TryGetValue(key, out MqttZeroSharedRouteEntries? existing)
                ? existing
                : new MqttZeroSharedRouteEntries(key, cursor);
        routeEntries = routeEntries.Set(session, subscription);
        return new MqttZeroRouteEndpoint(
            RegularEntries,
            entries.Set(key, routeEntries, out _));
    }

    /// <summary>
    /// 移除共享。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="session">会话。</param>
    /// <param name="removed">对应的 removed。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroRouteEndpoint? RemoveShared(
        MqttZeroSharedGroupKey key,
        MqttZeroSessionState session,
        out bool removed)
    {
        if (SharedEntries == null ||
            !SharedEntries.TryGetValue(
                key,
                out MqttZeroSharedRouteEntries? routeEntries))
        {
            removed = false;
            return this;
        }

        MqttZeroSharedRouteEntries? replacement = routeEntries.Remove(
            session,
            out removed);
        if (!removed)
        {
            return this;
        }

        SharedRouteMap entries =
            replacement == null
                ? SharedEntries.Remove(key, out _)
                : SharedEntries.Set(key, replacement, out _);
        MqttZeroRouteEndpoint result = new(
            RegularEntries,
            entries.Count > 0 ? entries : null);
        return result.IsEmpty ? null : result;
    }
}
