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

internal sealed class MqttZeroRouteEndpoint
{
    public static MqttZeroRouteEndpoint Empty { get; } = new(null, null);

    private MqttZeroRouteEndpoint(
        RegularRouteMap? regularEntries,
        SharedRouteMap? sharedEntries)
    {
        RegularEntries = regularEntries;
        SharedEntries = sharedEntries;
    }

    public RegularRouteMap? RegularEntries { get; }

    public SharedRouteMap? SharedEntries { get; }

    public bool IsEmpty
    {
        get
        {
            return (RegularEntries == null || RegularEntries.Count == 0) &&
                (SharedEntries == null || SharedEntries.Count == 0);
        }
    }

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

    public MqttZeroRouteEndpoint SetShared(
        MqttZeroSharedGroupKey key,
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        SharedRouteMap entries = SharedEntries ??
            SharedRouteMap.Create(
                EqualityComparer<MqttZeroSharedGroupKey>.Default);
        MqttZeroSharedRouteEntries routeEntries =
            entries.TryGetValue(key, out MqttZeroSharedRouteEntries? existing)
                ? existing
                : new MqttZeroSharedRouteEntries(key);
        routeEntries = routeEntries.Set(session, subscription);
        return new MqttZeroRouteEndpoint(
            RegularEntries,
            entries.Set(key, routeEntries, out _));
    }

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
