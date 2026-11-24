using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Collections.Concurrent;

namespace MqttZero.Server.Routing;

internal static class MqttZeroRouteSelector
{
    public static MqttZeroRouteScope Select(
        MqttZeroSubscriptionRouteSnapshot snapshot,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        ConcurrentDictionary<string, int> sharedCursors,
        MqttZeroRouteScopePool scopePool)
    {
        MqttZeroRouteScope route = scopePool.Rent();
        snapshot.TopicTrie.Match(topic, route);
        route.AddRegularGroups(snapshot, sender);

        for (int index = 0; index < snapshot.SharedGroups.Length; index++)
        {
            route.AddSharedGroup(
                snapshot.SharedGroups[index],
                topic,
                sender,
                sharedCursors);
        }

        return route;
    }

}
