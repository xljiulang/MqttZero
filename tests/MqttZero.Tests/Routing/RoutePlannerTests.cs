using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Tests.Routing;

public sealed class RoutePlannerTests
{
    [Fact]
    public void OneSessionMatchingThreeFiltersProducesOneDelivery()
    {
        MqttZeroSessionState session = new("subscriber", 100, 3);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(
                session,
                CreateSubscription(
                    "route/#",
                    MqttQualityOfServiceLevel.AtMostOnce,
                    1))
            .Add(
                session,
                CreateSubscription(
                    "route/+",
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    2))
            .Add(
                session,
                CreateSubscription(
                    "route/topic",
                    MqttQualityOfServiceLevel.ExactlyOnce,
                    3,
                    retainAsPublished: true));
        using MqttZeroRoutePlanBuffer buffer = new(8);

        MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
            epoch,
            "route/topic"u8,
            null,
            new MqttZeroSharedGroupCursors(),
            8,
            buffer);

        MqttZeroPlannedDelivery delivery =
            Assert.Single(plan.Batches[3].AsSpan().ToArray());
        Assert.Same(session, delivery.Session);
        Assert.Equal(
            MqttQualityOfServiceLevel.ExactlyOnce,
            delivery.QualityOfServiceLevel);
        Assert.True(delivery.RetainAsPublished);
        Assert.Equal([1U, 2U, 3U], delivery.SubscriptionIdentifiers.ToArray());
        Assert.Equal(1, plan.NonEmptyBatchCount);
        Assert.Equal(1, plan.TotalDeliveryCount);
    }

    [Fact]
    public void OneThousandSessionsProduceAtMostOneBatchPerShard()
    {
        using MqttZeroRoutePlanBuffer buffer = new(8);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty;
        for (int index = 0; index < 1_000; index++)
        {
            epoch = epoch.Add(
                new MqttZeroSessionState(
                    $"s-{index}",
                    100,
                    index % 8),
                CreateSubscription(
                    "fanout/#",
                    MqttQualityOfServiceLevel.AtMostOnce,
                    null));
        }

        MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
            epoch,
            "fanout/topic"u8,
            null,
            new MqttZeroSharedGroupCursors(),
            8,
            buffer);

        Assert.Equal(8, plan.NonEmptyBatchCount);
        Assert.Equal(1_000, plan.TotalDeliveryCount);
        for (int shardId = 0; shardId < plan.Batches.Length; shardId++)
        {
            Assert.Equal(shardId, plan.Batches[shardId].ShardId);
            Assert.Equal(125, plan.Batches[shardId].Count);
        }
    }

    [Fact]
    public void RegularNoLocalCandidateIsNotPlanned()
    {
        MqttZeroSessionState sender = new("sender", 100, 0);
        MqttZeroSessionState receiver = new("receiver", 100, 1);
        MqttZeroSubscription noLocal = CreateSubscription(
            "route/#",
            MqttQualityOfServiceLevel.AtLeastOnce,
            1,
            noLocal: true);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(sender, noLocal)
            .Add(
                receiver,
                CreateSubscription(
                    "route/#",
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    2));

        using MqttZeroRoutePlanBuffer buffer = new(8);
        MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
            epoch,
            "route/topic"u8,
            sender,
            new MqttZeroSharedGroupCursors(),
            8,
            buffer);

        MqttZeroPlannedDelivery delivery =
            Assert.Single(plan.Batches[1].AsSpan().ToArray());
        Assert.Same(receiver, delivery.Session);
        Assert.Equal(1, plan.TotalDeliveryCount);
    }

    [Fact]
    public void SharedDeliveryMergesWithExistingRegularDelivery()
    {
        MqttZeroSessionState session = new("subscriber", 100, 5);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(
                session,
                CreateSubscription(
                    "route/#",
                    MqttQualityOfServiceLevel.AtMostOnce,
                    11))
            .Add(
                session,
                CreateSharedSubscription(
                    "workers",
                    "route/#",
                    MqttQualityOfServiceLevel.ExactlyOnce,
                    22));

        using MqttZeroRoutePlanBuffer buffer = new(8);
        MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
            epoch,
            "route/topic"u8,
            null,
            new MqttZeroSharedGroupCursors(),
            8,
            buffer);

        MqttZeroPlannedDelivery delivery =
            Assert.Single(plan.Batches[5].AsSpan().ToArray());
        Assert.Equal(
            MqttQualityOfServiceLevel.ExactlyOnce,
            delivery.QualityOfServiceLevel);
        Assert.Equal(
            [11U, 22U],
            delivery.SubscriptionIdentifiers.ToArray());
        Assert.Equal(1, plan.TotalDeliveryCount);
    }

    [Fact]
    public void SharedNoLocalCandidateIsNotPlanned()
    {
        MqttZeroSessionState sender = new("sender", 100, 0);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty.Add(
            sender,
            CreateSharedSubscription(
                "workers",
                "route/#",
                MqttQualityOfServiceLevel.AtLeastOnce,
                1,
                noLocal: true));

        using MqttZeroRoutePlanBuffer buffer = new(8);
        MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
            epoch,
            "route/topic"u8,
            sender,
            new MqttZeroSharedGroupCursors(),
            8,
            buffer);

        Assert.Equal(0, plan.NonEmptyBatchCount);
        Assert.Equal(0, plan.TotalDeliveryCount);
    }

    [Fact]
    public void SharedNoLocalCandidatesAreRemovedBeforeOffsetSelection()
    {
        MqttZeroSessionState sender = new("sender", 100, 1);
        MqttZeroSessionState second = new("second", 100, 2);
        MqttZeroSessionState third = new("third", 100, 3);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(
                sender,
                CreateSharedSubscription(
                    "workers",
                    "route/#",
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    1,
                    noLocal: true))
            .Add(
                second,
                CreateSharedSubscription(
                    "workers",
                    "route/#",
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    2))
            .Add(
                third,
                CreateSharedSubscription(
                    "workers",
                    "route/#",
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    3));
        MqttZeroSharedGroupCursors cursors = new();
        using MqttZeroRoutePlanBuffer buffer = new(8);

        Dictionary<string, int> selections = new(StringComparer.Ordinal);
        for (int index = 0; index < 200; index++)
        {
            MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                epoch,
                "route/topic"u8,
                sender,
                cursors,
                8,
                buffer);
            Assert.Equal(1, plan.TotalDeliveryCount);
            MqttZeroPlannedDelivery delivery = GetOnlyDelivery(plan);
            string selected = delivery.Session.ClientId;
            selections[selected] =
                selections.GetValueOrDefault(selected) + 1;
        }

        Assert.False(selections.ContainsKey(sender.ClientId));
        Assert.Equal(100, selections["second"]);
        Assert.Equal(100, selections["third"]);
    }

    [Fact]
    public void ReusingBufferDoesNotRetainPreviousPlan()
    {
        using MqttZeroRoutePlanBuffer buffer = new(4);
        MqttZeroSessionState first = new("first", 100, 1);
        MqttZeroSessionState second = new("second", 100, 2);
        MqttZeroRouteEpoch firstEpoch = MqttZeroRouteEpoch.Empty.Add(
            first,
            CreateSubscription(
                "route/#",
                MqttQualityOfServiceLevel.AtLeastOnce,
                1));
        MqttZeroRouteEpoch secondEpoch = MqttZeroRouteEpoch.Empty.Add(
            second,
            CreateSubscription(
                "route/#",
                MqttQualityOfServiceLevel.AtLeastOnce,
                2));
        MqttZeroSharedGroupCursors cursors = new();

        _ = MqttZeroRoutePlanner.Plan(
            firstEpoch,
            "route/topic"u8,
            null,
            cursors,
            4,
            buffer);
        MqttZeroRoutePlan replacement = MqttZeroRoutePlanner.Plan(
            secondEpoch,
            "route/topic"u8,
            null,
            cursors,
            4,
            buffer);

        Assert.Equal(1, replacement.TotalDeliveryCount);
        Assert.Equal(0, replacement.Batches[1].Count);
        Assert.Same(
            second,
            Assert.Single(replacement.Batches[2].AsSpan().ToArray()).Session);
    }

    [Fact]
    public void PlanningDoesNotAllocateAfterWarmup()
    {
        MqttZeroRouteEpoch epoch = CreatePlanEpoch(100);
        using MqttZeroRoutePlannerLanes lanes = new(8);
        MqttZeroRoutePlanBuffer buffer = lanes[0];
        MqttZeroSharedGroupCursors cursors = new();
        for (int index = 0; index < 10; index++)
        {
            _ = MqttZeroRoutePlanner.Plan(
                epoch,
                "route/topic"u8,
                null,
                cursors,
                8,
                buffer);
        }

        int delivered = 0;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int index = 0; index < 1_000; index++)
        {
            MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                epoch,
                "route/topic"u8,
                null,
                cursors,
                8,
                buffer);
            delivered += plan.TotalDeliveryCount;
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(100_000, delivered);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void SharedPlanningDoesNotAllocateAfterWarmup()
    {
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty;
        for (int index = 0; index < 100; index++)
        {
            epoch = epoch.Add(
                new MqttZeroSessionState(
                    $"shared-{index}",
                    100,
                    index % 8),
                CreateSharedSubscription(
                    "workers",
                    "route/#",
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    (uint)index));
        }

        using MqttZeroRoutePlannerLanes lanes = new(8);
        MqttZeroRoutePlanBuffer buffer = lanes[0];
        MqttZeroSharedGroupCursors cursors = new();
        for (int index = 0; index < 200; index++)
        {
            _ = MqttZeroRoutePlanner.Plan(
                epoch,
                "route/topic"u8,
                null,
                cursors,
                8,
                buffer);
        }

        int delivered = 0;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int index = 0; index < 1_000; index++)
        {
            MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                epoch,
                "route/topic"u8,
                null,
                cursors,
                8,
                buffer);
            delivered += plan.TotalDeliveryCount;
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(1_000, delivered);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void RejectsSessionAssignedOutsideConfiguredShardCount()
    {
        using MqttZeroRoutePlanBuffer buffer = new(2);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty.Add(
            new MqttZeroSessionState("invalid", 100, 2),
            CreateSubscription(
                "route/#",
                MqttQualityOfServiceLevel.AtLeastOnce,
                null));

        Assert.Throws<InvalidOperationException>(() =>
        {
            _ = MqttZeroRoutePlanner.Plan(
                epoch,
                "route/topic"u8,
                null,
                new MqttZeroSharedGroupCursors(),
                2,
                buffer);
        });
    }

    [Fact]
    public void PlanBufferRejectsMismatchedShardCount()
    {
        using MqttZeroRoutePlanBuffer buffer = new(4);

        Assert.Throws<ArgumentException>(() => buffer.BeginPlan(3));
    }

    [Fact]
    public void LanesExposeIndependentBuffersAndRejectInvalidShards()
    {
        MqttZeroRoutePlannerLanes lanes = new(3);

        Assert.Equal(3, lanes.Count);
        Assert.NotSame(lanes[0], lanes[1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = lanes[-1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => _ = lanes[3]);

        lanes.Dispose();
        lanes.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _ = lanes[0]);
    }

    private static MqttZeroPlannedDelivery GetOnlyDelivery(
        MqttZeroRoutePlan plan)
    {
        for (int shardId = 0; shardId < plan.Batches.Length; shardId++)
        {
            MqttZeroFanoutBatch batch = plan.Batches[shardId];
            if (batch.Count == 1)
            {
                return batch[0];
            }
        }

        throw new InvalidOperationException("Expected exactly one delivery.");
    }

    private static MqttZeroRouteEpoch CreatePlanEpoch(int count)
    {
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty;
        for (int index = 0; index < count; index++)
        {
            epoch = epoch.Add(
                new MqttZeroSessionState(
                    $"plan-{index}",
                    100,
                    index % 8),
                CreateSubscription(
                    "route/#",
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    (uint)index));
        }

        return epoch;
    }

    private static MqttZeroSubscription CreateSubscription(
        string filter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        uint? identifier,
        bool noLocal = false,
        bool retainAsPublished = false)
    {
        return new MqttZeroSubscription(
            Encoding.UTF8.GetBytes(filter),
            qualityOfServiceLevel,
            noLocal,
            retainAsPublished,
            identifier);
    }

    private static MqttZeroSubscription CreateSharedSubscription(
        string group,
        string filter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        uint? identifier,
        bool noLocal = false,
        bool retainAsPublished = false)
    {
        return CreateSubscription(
            $"$share/{group}/{filter}",
            qualityOfServiceLevel,
            identifier,
            noLocal,
            retainAsPublished);
    }
}
