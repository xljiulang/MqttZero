using MqttZero.Protocol;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal static class MqttZeroRouteSelector
{
    public static MqttZeroRouteScope Select(
        MqttZeroSubscriptionRouteSnapshot snapshot,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        MqttZeroRouteScopePool scopePool)
    {
        MqttZeroRouteScope route = scopePool.Rent();
        route.EnsureMatchedGroupCapacity(
            snapshot.RegularGroups.Length);
        snapshot.TopicTrie.Match(topic, route);
        route.AddRegularGroups(snapshot, sender);

        for (int index = 0; index < snapshot.SharedGroups.Length; index++)
        {
            route.AddSharedGroup(
                snapshot.SharedGroups[index],
                topic,
                sender);
        }

        return route;
    }

}
