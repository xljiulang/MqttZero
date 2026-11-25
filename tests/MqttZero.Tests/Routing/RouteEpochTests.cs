using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Tests.Routing;

public sealed class RouteEpochTests
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
    public void EpochPreservesMqttWildcardSemantics(
        string filter,
        string topic,
        int expectedCount)
    {
        MqttZeroSessionState session = new("receiver");
        MqttZeroSubscription subscription = CreateSubscription(filter);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(session, subscription);
        MqttZeroRouteEpochMatch match = new();

        epoch.Match(Encoding.UTF8.GetBytes(topic), null, ref match);

        Assert.Equal(expectedCount, match.Count);
    }

    [Fact]
    public void UpdatingOneFilterSharesUnchangedTriePaths()
    {
        MqttZeroSessionState a = new("a");
        MqttZeroSessionState b = new("b");
        MqttZeroSubscription alphaOne = CreateSubscription("alpha/one");
        MqttZeroSubscription alphaTwo = CreateSubscription("alpha/two");
        MqttZeroSubscription beta = CreateSubscription("beta/#");

        MqttZeroRouteEpoch first = MqttZeroRouteEpoch.Empty
            .Add(a, alphaOne)
            .Add(a, alphaTwo)
            .Add(b, beta);
        MqttZeroRouteEpoch second = first.Add(
            b,
            CreateSubscription("alpha/one"));

        Assert.Same(
            first.GetNodeForTest("beta/#"u8),
            second.GetNodeForTest("beta/#"u8));
        Assert.NotSame(
            first.GetNodeForTest("alpha/one"u8),
            second.GetNodeForTest("alpha/one"u8));
        Assert.Equal(first.Version + 1, second.Version);
    }

    [Fact]
    public void RemovingMissingSubscriptionReturnsSameEpoch()
    {
        MqttZeroSessionState session = new("receiver");
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(session, CreateSubscription("missing/#"));

        MqttZeroRouteEpoch unchanged = epoch.Remove(
            session,
            CreateSubscription("other/#"));

        Assert.Same(epoch, unchanged);
    }

    [Fact]
    public void RegularNoLocalFiltersOnlyTheSender()
    {
        MqttZeroSessionState sender = new("sender");
        MqttZeroSessionState receiver = new("receiver");
        MqttZeroSubscription noLocal = CreateSubscription(
            "route/#",
            noLocal: true);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(sender, noLocal)
            .Add(receiver, noLocal);

        MqttZeroRouteEpochMatch match = new();
        epoch.Match("route/topic"u8, sender, ref match);

        Assert.Equal(1, match.Count);
        MqttZeroDeliveryCandidate delivery = match[0];
        Assert.Same(receiver, delivery.Session);
    }

    [Fact]
    public void SharedGroupSelectsOneSessionPerGroup()
    {
        MqttZeroSessionState a = new("a");
        MqttZeroSessionState b = new("b");
        MqttZeroSessionState c = new("c");
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(a, CreateSubscription("$share/workers/jobs/#"))
            .Add(b, CreateSubscription("$share/workers/jobs/#"))
            .Add(c, CreateSubscription("$share/audit/jobs/#"));

        MqttZeroRouteEpochMatch match = new();
        epoch.Match("jobs/one"u8, null, ref match);

        Assert.Equal(2, match.Count);
        HashSet<MqttZeroSessionState> sessions = [];
        for (int index = 0; index < match.Count; index++)
        {
            sessions.Add(match[index].Session);
        }

        Assert.Equal(2, sessions.Count);
    }

    [Fact]
    public void SharedNoLocalCanRemoveTheOnlyGroupCandidate()
    {
        MqttZeroSessionState sender = new("sender");
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty.Add(
            sender,
            CreateSubscription(
                "$share/workers/jobs/#",
                noLocal: true));
        MqttZeroRouteEpochMatch match = new();

        epoch.Match("jobs/one"u8, sender, ref match);

        Assert.Equal(0, match.Count);
    }

    [Fact]
    public void SharedGroupsWithDifferentFiltersRemainDistinct()
    {
        MqttZeroSessionState a = new("a");
        MqttZeroSessionState b = new("b");
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(a, CreateSubscription("$share/workers/jobs/#"))
            .Add(b, CreateSubscription("$share/workers/+/one"));
        MqttZeroRouteEpochMatch match = new();

        epoch.Match("jobs/one"u8, null, ref match);

        Assert.Equal(2, match.Count);
    }

    [Fact]
    public void MatchPreservesCandidatesAcrossRegularAndSharedFilters()
    {
        MqttZeroSessionState session = new("receiver");
        MqttZeroSubscription regular = CreateSubscription(
            "route/#",
            identifier: 7);
        MqttZeroSubscription shared = CreateSubscription(
            "$share/workers/route/#",
            identifier: 9);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(session, regular)
            .Add(session, shared);
        MqttZeroRouteEpochMatch match = new();

        epoch.Match("route/topic"u8, null, ref match);

        Assert.Equal(2, match.Count);
        List<uint> identifiers = [];
        for (int index = 0; index < match.Count; index++)
        {
            identifiers.AddRange(
                match[index].SubscriptionIdentifiers.ToArray());
        }

        identifiers.Sort();
        Assert.Equal(
            new uint[] { 7, 9 },
            identifiers);
    }

    [Fact]
    public void UpdatingSubscriptionDoesNotMutatePreviousEpoch()
    {
        MqttZeroSessionState session = new("receiver");
        MqttZeroSubscription first = CreateSubscription(
            "route/#",
            qualityOfServiceLevel: MqttQualityOfServiceLevel.AtMostOnce);
        MqttZeroSubscription second = CreateSubscription(
            "route/#",
            qualityOfServiceLevel: MqttQualityOfServiceLevel.ExactlyOnce);
        MqttZeroRouteEpoch firstEpoch = MqttZeroRouteEpoch.Empty
            .Add(session, first);
        MqttZeroRouteEpoch secondEpoch = firstEpoch.Add(session, second);

        MqttZeroRouteEpochMatch firstMatch = new();
        MqttZeroRouteEpochMatch secondMatch = new();
        firstEpoch.Match("route/topic"u8, null, ref firstMatch);
        secondEpoch.Match("route/topic"u8, null, ref secondMatch);

        Assert.Same(first, firstMatch[0].Subscription);
        Assert.Same(second, secondMatch[0].Subscription);
    }

    [Fact]
    public void RemoveDeletesOnlyTheSelectedEndpointEntry()
    {
        MqttZeroSessionState session = new("receiver");
        MqttZeroSubscription regular = CreateSubscription("route/#");
        MqttZeroSubscription shared = CreateSubscription(
            "$share/workers/route/#");
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(session, regular)
            .Add(session, shared);
        MqttZeroRouteEpoch regularRemoved = epoch.Remove(session, regular);
        MqttZeroRouteEpoch allRemoved = regularRemoved.Remove(session, shared);
        MqttZeroRouteEpochMatch regularRemovedMatch = new();
        MqttZeroRouteEpochMatch allRemovedMatch = new();

        regularRemoved.Match("route/topic"u8, null, ref regularRemovedMatch);
        allRemoved.Match("route/topic"u8, null, ref allRemovedMatch);

        Assert.Equal(1, regularRemovedMatch.Count);
        Assert.True(regularRemovedMatch[0].Session == session);
        Assert.Equal(0, allRemovedMatch.Count);
    }

    [Fact]
    public void SharedGroupKeyUsesGroupAndFilterBytes()
    {
        MqttZeroSharedGroupKey key = new("workers"u8, "jobs/#"u8);
        MqttZeroSharedGroupKey equal = new("workers"u8, "jobs/#"u8);
        MqttZeroSharedGroupKey otherGroup = new("audit"u8, "jobs/#"u8);
        MqttZeroSharedGroupKey otherFilter = new("workers"u8, "jobs/+"u8);

        Assert.Equal(key, equal);
        Assert.Equal(key.GetHashCode(), equal.GetHashCode());
        Assert.NotEqual(key, otherGroup);
        Assert.NotEqual(key, otherFilter);
    }

    [Fact]
    public void TopicSegmentUsesRawUtf8Bytes()
    {
        MqttZeroTopicSegment segment = MqttZeroTopicSegment.From(
            "设备/温度"u8);
        MqttZeroTopicSegment equal = MqttZeroTopicSegment.From(
            "设备/温度"u8);
        MqttZeroTopicSegment other = MqttZeroTopicSegment.From(
            "设备/湿度"u8);

        Assert.Equal(segment, equal);
        Assert.Equal(segment.GetHashCode(), equal.GetHashCode());
        Assert.NotEqual(segment, other);
        Assert.True(segment.Span.SequenceEqual("设备/温度"u8));
    }

    private static MqttZeroSubscription CreateSubscription(
        string filter,
        bool noLocal = false,
        uint? identifier = null,
        MqttQualityOfServiceLevel qualityOfServiceLevel =
            MqttQualityOfServiceLevel.AtLeastOnce)
    {
        return new MqttZeroSubscription(
            Encoding.UTF8.GetBytes(filter),
            qualityOfServiceLevel,
            noLocal,
            false,
            identifier);
    }
}
