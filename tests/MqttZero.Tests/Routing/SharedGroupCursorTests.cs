using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Tests.Routing;

public sealed class SharedGroupCursorTests
{
    [Fact]
    public void CursorSurvivesEquivalentEpochRebuild()
    {
        MqttZeroSessionState a = new("a");
        MqttZeroSessionState b = new("b");
        MqttZeroRouteEpoch first = MqttZeroRouteEpoch.Empty
            .Add(a, CreateShared("workers", "jobs/#"))
            .Add(b, CreateShared("workers", "jobs/#"));

        string firstSelected = SelectSingle(first);

        MqttZeroRouteEpoch replacement = first
            .Remove(a, CreateShared("workers", "jobs/#"))
            .Remove(b, CreateShared("workers", "jobs/#"))
            .Add(a, CreateShared("workers", "jobs/#"))
            .Add(b, CreateShared("workers", "jobs/#"));
        string secondSelected = SelectSingle(replacement);

        Assert.NotSame(first, replacement);
        Assert.NotEqual(firstSelected, secondSelected);
        Assert.Equal(
            ["a", "b"],
            new[] { firstSelected, secondSelected }.Order());
    }

    [Fact]
    public void GroupAndFilterKeysUseIndependentCursors()
    {
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroSharedGroupKey workersJobs =
            new("workers"u8, "jobs/#"u8);
        MqttZeroSharedGroupKey auditJobs =
            new("audit"u8, "jobs/#"u8);
        MqttZeroSharedGroupKey workersOther =
            new("workers"u8, "jobs/+"u8);

        MqttZeroSharedGroupCursor workersJobsCursor =
            cursors.Resolve(workersJobs);
        Assert.Equal(0, workersJobsCursor.NextOffset(2));
        Assert.Equal(1, workersJobsCursor.NextOffset(2));
        Assert.Equal(0, cursors.Resolve(auditJobs).NextOffset(2));
        Assert.Equal(0, cursors.Resolve(workersOther).NextOffset(2));
        Assert.Equal(2, workersJobsCursor.Value);
        Assert.Equal(1, cursors.Resolve(auditJobs).Value);
        Assert.Equal(1, cursors.Resolve(workersOther).Value);
    }

    [Fact]
    public void NoLocalCandidatesAreRemovedBeforeOffsetSelection()
    {
        MqttZeroSessionState sender = new("sender");
        MqttZeroSessionState second = new("second");
        MqttZeroSessionState third = new("third");
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(sender, CreateShared("workers", "jobs/#", noLocal: true))
            .Add(second, CreateShared("workers", "jobs/#"))
            .Add(third, CreateShared("workers", "jobs/#"));

        Dictionary<string, int> selections = new(StringComparer.Ordinal);
        for (int index = 0; index < 200; index++)
        {
            string selected = SelectSingle(epoch, sender);
            selections[selected] =
                selections.GetValueOrDefault(selected) + 1;
        }

        Assert.False(selections.ContainsKey(sender.ClientId));
        Assert.Equal(100, selections["second"]);
        Assert.Equal(100, selections["third"]);
    }

    [Fact]
    public void RemovedGroupNoLongerMatchesOrKeepsItsCursor()
    {
        MqttZeroSessionState a = new("a");
        MqttZeroSessionState b = new("b");
        MqttZeroSubscription workersJobs = CreateShared("workers", "jobs/#");
        MqttZeroSubscription auditJobs = CreateShared("audit", "jobs/#");
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(a, workersJobs)
            .Add(a, auditJobs)
            .Add(b, workersJobs);
        HashSet<MqttZeroSharedGroupKey> keys = [];
        epoch.CollectSharedGroupKeys(keys);
        Assert.Equal(2, keys.Count);

        MqttZeroRouteEpoch withoutAudit =
            epoch.Remove(a, auditJobs);

        HashSet<MqttZeroSharedGroupKey> remaining = [];
        withoutAudit.CollectSharedGroupKeys(remaining);
        MqttZeroSharedGroupKey auditKey =
            new("audit"u8, "jobs/#"u8);
        MqttZeroSharedGroupKey workersKey =
            new("workers"u8, "jobs/#"u8);
        Assert.DoesNotContain(auditKey, remaining);
        Assert.Contains(workersKey, remaining);
    }

    [Fact]
    public void CursorWrapsAcrossOffsets()
    {
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroSharedGroupKey key = new("workers"u8, "jobs/#"u8);
        MqttZeroSharedGroupCursor cursor = cursors.Resolve(key);

        int[] offsets =
        [
            cursor.NextOffset(3),
            cursor.NextOffset(3),
            cursor.NextOffset(3),
            cursor.NextOffset(3),
            cursor.NextOffset(3)
        ];

        Assert.Equal([0, 1, 2, 0, 1], offsets);
    }

    [Fact]
    public void CursorRejectsNonPositiveCandidateCount()
    {
        MqttZeroSharedGroupCursor cursor = new();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            cursor.NextOffset(0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            cursor.NextOffset(-1));
    }

    [Fact]
    public void CursorSnapshotReportsActiveKeyCount()
    {
        MqttZeroSharedGroupCursors cursors = new();
        Assert.Equal(0, cursors.Count);

        Assert.Equal(
            0,
            cursors.Resolve(new MqttZeroSharedGroupKey(
                "workers"u8,
                "jobs/#"u8)).NextOffset(2));
        Assert.Equal(
            0,
            cursors.Resolve(new MqttZeroSharedGroupKey(
                "audit"u8,
                "jobs/#"u8)).NextOffset(2));

        Assert.Equal(2, cursors.Count);
    }

    [Fact]
    public void PruneRemovesInactiveKeysAndPreservesActiveCursor()
    {
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroSharedGroupKey active =
            new("workers"u8, "jobs/#"u8);
        MqttZeroSharedGroupKey removed =
            new("audit"u8, "jobs/#"u8);

        MqttZeroSharedGroupCursor activeCursor = cursors.Resolve(active);
        Assert.Equal(0, activeCursor.NextOffset(2));
        Assert.Equal(1, activeCursor.NextOffset(2));
        Assert.Equal(0, cursors.Resolve(removed).NextOffset(2));

        cursors.PruneTo([active]);

        Assert.Equal(1, cursors.Count);
        Assert.Equal(2, activeCursor.Value);
    }

    [Fact]
    public void RouteEpochCollectsOnlyActiveSharedGroupKeys()
    {
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(
                new MqttZeroSessionState("a"),
                CreateShared("workers", "jobs/#"))
            .Add(
                new MqttZeroSessionState("b"),
                new MqttZeroSubscription(
                    "regular/#"u8.ToArray(),
                    MqttQualityOfServiceLevel.AtMostOnce,
                    false,
                    false,
                    null));
        HashSet<MqttZeroSharedGroupKey> keys = [];

        epoch.CollectSharedGroupKeys(keys);

        Assert.Equal(
            new MqttZeroSharedGroupKey(
                "workers"u8,
                "jobs/#"u8),
            Assert.Single(keys));
    }

    [Fact]
    public void PruneKeepsOnlyCurrentKeyAfterManySharedGroupChurns()
    {
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroSharedGroupKey active = default;
        for (int index = 0; index < 1_000; index++)
        {
            MqttZeroSharedGroupKey key = new(
                Encoding.UTF8.GetBytes($"group-{index}"),
                "jobs/#"u8);
            active = key;
            _ = cursors.Resolve(key).NextOffset(2);
        }

        Assert.Equal(1_000, cursors.Count);
        cursors.PruneTo([active]);
        Assert.Equal(1, cursors.Count);
        Assert.Equal(1, cursors.Resolve(active).Value);
    }

    [Fact]
    public void SharedRoutingDoesNotAllocateAfterWarmup()
    {
        MqttZeroSessionState first = new("first");
        MqttZeroSessionState second = new("second");
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(first, CreateShared("workers", "jobs/#"))
            .Add(second, CreateShared("workers", "jobs/#"));
        using MqttZeroRoutePlannerLanes lanes = new(1);
        MqttZeroRoutePlanBuffer buffer = lanes[0];

        for (int index = 0; index < 10; index++)
        {
            MqttZeroRoutePlan warmup = MqttZeroRoutePlanner.Plan(
                epoch,
                "jobs/one"u8,
                null,
                1,
                buffer);
            Assert.Equal(1, warmup.TotalDeliveryCount);
        }

        int delivered = 0;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int index = 0; index < 100; index++)
        {
            MqttZeroRoutePlan route = MqttZeroRoutePlanner.Plan(
                epoch,
                "jobs/one"u8,
                null,
                1,
                buffer);
            delivered += route.TotalDeliveryCount;
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(100, delivered);
        Assert.Equal(0, allocated);
    }

    private static string SelectSingle(
        MqttZeroRouteEpoch epoch,
        MqttZeroSessionState? sender = null)
    {
        using MqttZeroRoutePlannerLanes lanes = new(1);
        MqttZeroRoutePlan route = MqttZeroRoutePlanner.Plan(
            epoch,
            "jobs/one"u8,
            sender,
            1,
            lanes[0]);
        Assert.Equal(1, route.TotalDeliveryCount);
        for (int shardId = 0; shardId < route.Batches.Length; shardId++)
        {
            MqttZeroFanoutBatch batch = route.Batches[shardId];
            if (batch.Count > 0)
            {
                return batch[0].Session.ClientId;
            }
        }

        throw new InvalidOperationException("Expected one shared delivery.");
    }

    private static MqttZeroSubscription CreateShared(
        string group,
        string filter,
        bool noLocal = false)
    {
        return new MqttZeroSubscription(
            Encoding.UTF8.GetBytes($"$share/{group}/{filter}"),
            MqttQualityOfServiceLevel.AtLeastOnce,
            noLocal,
            false,
            null);
    }
}
