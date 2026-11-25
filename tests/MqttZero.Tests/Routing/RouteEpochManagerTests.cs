using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Tests.Routing;

public sealed class RouteEpochManagerTests
{
    [Fact]
    public void BuildAddAndBuildRemoveAreRelativeToCurrentEpoch()
    {
        MqttZeroRouteEpochManager manager = new();
        MqttZeroSessionState session = new("subscriber");
        MqttZeroSubscription subscription = CreateSubscription("route/#");

        MqttZeroRouteEpoch added = manager.BuildAdd(
            [subscription],
            session);
        manager.Publish(MqttZeroRouteEpoch.Empty, added);
        MqttZeroRouteEpoch removed = manager.BuildRemove(
            [subscription],
            session);
        manager.Publish(added, removed);

        Assert.Same(removed, manager.Current);
        Assert.Equal(2, removed.Version);
    }

    [Fact]
    public void PublishRejectsStaleReplacement()
    {
        MqttZeroRouteEpochManager manager = new();
        MqttZeroSessionState session = new("subscriber");
        MqttZeroSubscription subscription = CreateSubscription("route/#");
        MqttZeroRouteEpoch first = manager.BuildAdd(
            [subscription],
            session);
        manager.Publish(MqttZeroRouteEpoch.Empty, first);
        MqttZeroRouteEpoch replacement = manager.BuildAdd(
            [CreateSubscription("route/+")],
            session);

        Assert.Throws<InvalidOperationException>(() =>
        {
            manager.Publish(MqttZeroRouteEpoch.Empty, replacement);
        });
        Assert.Same(first, manager.Current);
        Assert.NotSame(replacement, manager.Current);
    }

    [Fact]
    public void BuildFromSessionsAddsEverySubscription()
    {
        MqttZeroSessionState first = new("first");
        first.Subscriptions.Add(CreateSubscription("one/#"));
        MqttZeroSessionState second = new("second");
        second.Subscriptions.Add(CreateSubscription("two/#"));

        MqttZeroRouteEpoch epoch =
            MqttZeroRouteEpochManager.BuildFromSessions([first, second]);

        Assert.Equal(2, epoch.Version);
    }

    private static MqttZeroSubscription CreateSubscription(string filter)
    {
        return new MqttZeroSubscription(
            Encoding.UTF8.GetBytes(filter),
            MqttQualityOfServiceLevel.AtMostOnce,
            false,
            false,
            null);
    }
}
