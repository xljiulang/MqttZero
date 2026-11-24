using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;

namespace MqttZero.Tests.Routing
{
    public sealed class RouteAllocationTests
    {
        [Fact]
        public void RoutesOneSubscriberWithoutAllocatingAfterWarmup()
        {
            AssertZeroAllocationForSubscriberCount(1);
        }

        [Fact]
        public void RoutesOneHundredSubscribersWithoutAllocatingAfterWarmup()
        {
            AssertZeroAllocationForSubscriberCount(100);
        }

        [Fact]
        public void RoutesOneThousandSubscribersWithoutAllocatingAfterWarmup()
        {
            AssertZeroAllocationForSubscriberCount(1000);
        }

        [Fact]
        public void SharedSubscriptionSelectsOneSessionPerGroup()
        {
            List<MqttZeroSessionState> sessions =
            [
                CreateSession("a", "$share/workers/jobs/#"),
                CreateSession("b", "$share/workers/jobs/#"),
                CreateSession("c", "$share/audit/jobs/#")
            ];
            MqttZeroSubscriptionRouteSnapshot snapshot =
                MqttZeroSubscriptionRouteSnapshotBuilder.Build(sessions);
            MqttZeroRouteScopePool scopePool = new();

            using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                snapshot,
                "jobs/one"u8,
                null,
                scopePool);

            Assert.Equal(2, route.Count);
            Assert.Equal("a", route[0].Session.ClientId);
            Assert.Equal("c", route[1].Session.ClientId);
        }

        [Fact]
        public void SharedSubscriptionsWithDifferentFiltersAreDistinct()
        {
            List<MqttZeroSessionState> sessions =
            [
                CreateSession("a", "$share/workers/jobs/#"),
                CreateSession("b", "$share/workers/+/one")
            ];
            MqttZeroSubscriptionRouteSnapshot snapshot =
                MqttZeroSubscriptionRouteSnapshotBuilder.Build(sessions);
            MqttZeroRouteScopePool scopePool = new();

            using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                snapshot,
                "jobs/one"u8,
                null,
                scopePool);

            Assert.Equal(2, route.Count);
        }

        [Fact]
        public void SharedSubscriptionSelectsHighestMatchingQosAndHonorsNoLocal()
        {
            MqttZeroSessionState first = new("a");
            MqttZeroSessionState second = new("b");
            MqttZeroSubscription lowQos = new(
                "$share/workers/route/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null);
            MqttZeroSubscription highQos = new(
                "$share/workers/route/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null);
            MqttZeroSubscription noLocal = new(
                "$share/workers/route/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                true,
                false,
                null);
            MqttZeroSubscription secondOnly = new(
                "$share/workers/route/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null);
            MqttZeroSharedRouteGroup group = new(
                "workers",
                [
                    new MqttZeroSharedRouteEntry(first, [lowQos, highQos, noLocal]),
                    new MqttZeroSharedRouteEntry(second, [secondOnly])
                ]);
            MqttZeroSubscriptionRouteSnapshot snapshot = new([], [group]);
            MqttZeroRouteScopePool scopePool = new();

            using (MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                snapshot,
                "route/topic"u8,
                second,
                scopePool))
            {
                Assert.Equal(1, route.Count);
                MqttZeroDeliveryCandidate delivery = route[0];
                Assert.Same(first, delivery.Session);
                Assert.Same(highQos, delivery.Subscription);
            }

            using (MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                snapshot,
                "route/topic"u8,
                first,
                scopePool))
            {
                Assert.Equal(1, route.Count);
                MqttZeroDeliveryCandidate delivery = route[0];
                Assert.Same(second, delivery.Session);
            }
        }

        [Fact]
        public void SharedSubscriptionRoutingDoesNotAllocateAfterWarmup()
        {
            MqttZeroSharedRouteEntry[] entries =
                new MqttZeroSharedRouteEntry[100];
            for (int index = 0; index < entries.Length; index++)
            {
                MqttZeroSubscription subscription = new(
                    "$share/workers/route/#"u8.ToArray(),
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    false,
                    false,
                    null);
                entries[index] = new MqttZeroSharedRouteEntry(
                    new MqttZeroSessionState(
                        index.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                    [subscription]);
            }

            MqttZeroSubscriptionRouteSnapshot snapshot = new(
                [],
                [new MqttZeroSharedRouteGroup(
                    "workers",
                    entries)]);
            MqttZeroRouteScopePool scopePool = new();
            for (int index = 0; index < 10; index++)
            {
                using MqttZeroRouteScope warmup = MqttZeroRouteSelector.Select(
                    snapshot,
                    "route/topic"u8,
                    null,
                    scopePool);
                Assert.Equal(1, warmup.Count);
            }

            int delivered = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 100; index++)
            {
                using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                    snapshot,
                    "route/topic"u8,
                    null,
                    scopePool);
                delivered += route.Count;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(100, delivered);
            Assert.Equal(0, allocated);
        }

        private static void AssertZeroAllocationForSubscriberCount(int subscriberCount)
        {
            List<MqttZeroSessionState> sessions = new(subscriberCount);
            for (int index = 0; index < subscriberCount; index++)
            {
                sessions.Add(CreateSession(
                    index.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    "route/#"));
            }

            MqttZeroSubscriptionRouteSnapshot snapshot =
                MqttZeroSubscriptionRouteSnapshotBuilder.Build(sessions);
            MqttZeroRouteScopePool scopePool = new();
            for (int index = 0; index < 10; index++)
            {
                using (MqttZeroRouteScope warmup = MqttZeroRouteSelector.Select(
                    snapshot,
                    "route/topic"u8,
                    null,
                    scopePool))
                {
                    Assert.Equal(subscriberCount, warmup.Count);
                }
            }

            int delivered = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 100; index++)
            {
                using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                    snapshot,
                    "route/topic"u8,
                    null,
                    scopePool);
                delivered += route.Count;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(subscriberCount * 100, delivered);
            Assert.Equal(0, allocated);
        }

        private static MqttZeroSessionState CreateSession(string clientId, string topicFilter)
        {
            MqttZeroSessionState session = new(clientId);
            session.Subscriptions.Add(new MqttZeroSubscription(
                System.Text.Encoding.UTF8.GetBytes(topicFilter),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            return session;
        }
    }
}
