using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;

namespace MqttZero.Tests.Routing
{
    public sealed class TopicTrieTests
    {
        [Theory]
        [InlineData("foo", "foo", 1)]
        [InlineData("foo", "foo/", 0)]
        [InlineData("foo/+", "foo/", 1)]
        [InlineData("foo/bar", "foo/bar/", 0)]
        [InlineData("foo/#", "foo", 1)]
        [InlineData("foo/#", "foo/", 1)]
        [InlineData("foo/#", "foo/bar", 1)]
        [InlineData("+/bar", "/bar", 1)]
        public void MatchesTrailingEmptyLevels(
            string filter,
            string topic,
            int expectedCount)
        {
            MqttZeroSessionState session = new("receiver");
            session.Subscriptions.Add(new MqttZeroSubscription(
                System.Text.Encoding.UTF8.GetBytes(filter),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            MqttZeroSubscriptionRouteSnapshot snapshot =
                MqttZeroSubscriptionRouteSnapshotBuilder.Build([session]);
            using MqttZeroRouteScopePool pool = new();

            using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                snapshot,
                System.Text.Encoding.UTF8.GetBytes(topic),
                null,
                pool);

            Assert.Equal(expectedCount, route.Count);
        }
    }
}
