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
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroRouteEpoch first = MqttZeroRouteEpoch.Empty
            .Add(a, CreateShared("workers", "jobs/#"))
            .Add(b, CreateShared("workers", "jobs/#"));

        string firstSelected = SelectSingle(first, cursors);

        MqttZeroRouteEpoch replacement = MqttZeroRouteEpoch.Empty
            .Add(a, CreateShared("workers", "jobs/#"))
            .Add(b, CreateShared("workers", "jobs/#"));
        string secondSelected = SelectSingle(replacement, cursors);

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

        Assert.Equal(0, cursors.GetNext(workersJobs, 2));
        Assert.Equal(1, cursors.GetNext(workersJobs, 2));
        Assert.Equal(0, cursors.GetNext(auditJobs, 2));
        Assert.Equal(0, cursors.GetNext(workersOther, 2));
        Assert.Equal(2, cursors.GetCursor(workersJobs));
        Assert.Equal(1, cursors.GetCursor(auditJobs));
        Assert.Equal(1, cursors.GetCursor(workersOther));
    }

    [Fact]
    public void NoLocalCandidatesAreRemovedBeforeOffsetSelection()
    {
        MqttZeroSessionState sender = new("sender");
        MqttZeroSessionState second = new("second");
        MqttZeroSessionState third = new("third");
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(sender, CreateShared("workers", "jobs/#", noLocal: true))
            .Add(second, CreateShared("workers", "jobs/#"))
            .Add(third, CreateShared("workers", "jobs/#"));

        Dictionary<string, int> selections = new(StringComparer.Ordinal);
        for (int index = 0; index < 200; index++)
        {
            string selected = SelectSingle(epoch, cursors, sender);
            selections[selected] =
                selections.GetValueOrDefault(selected) + 1;
        }

        Assert.False(selections.ContainsKey(sender.ClientId));
        Assert.Equal(100, selections["second"]);
        Assert.Equal(100, selections["third"]);
    }

    [Fact]
    public void CursorWrapsAcrossOffsets()
    {
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroSharedGroupKey key = new("workers"u8, "jobs/#"u8);

        int[] offsets =
        [
            cursors.GetNext(key, 3),
            cursors.GetNext(key, 3),
            cursors.GetNext(key, 3),
            cursors.GetNext(key, 3),
            cursors.GetNext(key, 3)
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
    public void SharedRoutingDoesNotAllocateAfterWarmup()
    {
        MqttZeroSessionState first = new("first");
        MqttZeroSessionState second = new("second");
        MqttZeroSharedGroupCursors cursors = new();
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(first, CreateShared("workers", "jobs/#"))
            .Add(second, CreateShared("workers", "jobs/#"));
        MqttZeroRouteScopePool pool = new();

        for (int index = 0; index < 10; index++)
        {
            using MqttZeroRouteScope warmup = MqttZeroRouteSelector.Select(
                epoch,
                "jobs/one"u8,
                null,
                cursors,
                pool);
            Assert.Equal(1, warmup.Count);
        }

        int delivered = 0;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int index = 0; index < 100; index++)
        {
            using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                epoch,
                "jobs/one"u8,
                null,
                cursors,
                pool);
            delivered += route.Count;
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(100, delivered);
        Assert.Equal(0, allocated);
    }

    private static string SelectSingle(
        MqttZeroRouteEpoch epoch,
        MqttZeroSharedGroupCursors cursors,
        MqttZeroSessionState? sender = null)
    {
        using MqttZeroRouteScopePool pool = new();
        using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
            epoch,
            "jobs/one"u8,
            sender,
            cursors,
            pool);
        Assert.Equal(1, route.Count);
        return route[0].Session.ClientId;
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
