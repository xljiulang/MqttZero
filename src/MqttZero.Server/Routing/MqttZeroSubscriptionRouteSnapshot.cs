namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSubscriptionRouteSnapshot
{
    public static MqttZeroSubscriptionRouteSnapshot Empty { get; } =
        new([], []);

    public MqttZeroSubscriptionRouteSnapshot(
        MqttZeroRouteFilterGroup[] regularGroups,
        MqttZeroSharedRouteGroup[] sharedGroups)
    {
        RegularGroups = regularGroups;
        SharedGroups = sharedGroups;
        TopicTrie = new MqttZeroTopicTrie(regularGroups);
    }

    public MqttZeroRouteFilterGroup[] RegularGroups { get; }

    public MqttZeroSharedRouteGroup[] SharedGroups { get; }

    public MqttZeroTopicTrie TopicTrie { get; }
}
