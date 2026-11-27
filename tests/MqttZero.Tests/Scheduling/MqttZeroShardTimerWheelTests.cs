using MqttZero.Server.Scheduling;
using System.Globalization;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroShardTimerWheelTests
{
    private static readonly TimeSpan TickInterval =
        TimeSpan.FromMilliseconds(100);

    [Fact(Timeout = 5_000)]
    public async Task DeadlinesBeforeEqualAndAfterCurrentTickFireAtTheRightTick()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        MqttZeroTimerNode before = CreateNode("before", now);
        MqttZeroTimerNode equal = CreateNode("equal", now);
        MqttZeroTimerNode after = CreateNode("after", now + TickInterval);
        long currentTick = now.UtcTicks / TickInterval.Ticks;
        wheel.ScheduleOrRemove(
            before,
            (currentTick - 1) * TickInterval.Ticks);
        wheel.ScheduleOrRemove(
            equal,
            currentTick * TickInterval.Ticks);
        wheel.ScheduleOrRemove(
            after,
            (currentTick + 1) * TickInterval.Ticks);

        await TickOnShardAsync(
            shard,
            wheel,
            now,
            cancellationToken);

        Assert.Equal([before, equal], fired);
        Assert.True(after.IsScheduled);
        Assert.Equal(1, wheel.Count);

        await TickOnShardAsync(
            shard,
            wheel,
            now + TickInterval,
            cancellationToken);

        Assert.Equal([before, equal, after], fired);
        Assert.Equal(0, wheel.Count);
    }

    [Fact(Timeout = 5_000)]
    public async Task DeadlinesUseFourHierarchicalLevelsAtTheirBoundaries()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            []);
        long currentTick = now.UtcTicks / TickInterval.Ticks;
        MqttZeroTimerNode level0 = CreateNode("level0", now);
        MqttZeroTimerNode level1 = CreateNode("level1", now);
        MqttZeroTimerNode level2 = CreateNode("level2", now);
        MqttZeroTimerNode level3 = CreateNode("level3", now);

        wheel.ScheduleOrRemove(
            level0,
            (currentTick + 511) * TickInterval.Ticks);
        wheel.ScheduleOrRemove(
            level1,
            (currentTick + 512) * TickInterval.Ticks);
        wheel.ScheduleOrRemove(
            level2,
            (currentTick + (512L * 512)) * TickInterval.Ticks);
        wheel.ScheduleOrRemove(
            level3,
            (currentTick + (512L * 512 * 512)) * TickInterval.Ticks);

        Assert.Equal(0, level0.Level);
        Assert.Equal(1, level1.Level);
        Assert.Equal(2, level2.Level);
        Assert.Equal(3, level3.Level);
        Assert.Equal(4, wheel.Count);
        cancellationToken.ThrowIfCancellationRequested();
    }

    [Fact(Timeout = 5_000)]
    public async Task LevelOneDeadlineCascadesAndFiresAtItsDueTick()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        MqttZeroTimerNode node = CreateNode("level-one", now);
        DateTimeOffset dueAt = now.AddTicks(
            TickInterval.Ticks * 512);
        wheel.ScheduleOrRemove(node, dueAt.UtcTicks);
        Assert.Equal(1, node.Level);

        await TickOnShardAsync(
            shard,
            wheel,
            dueAt,
            cancellationToken);

        Assert.Equal([node], fired);
        Assert.Equal(0, wheel.Count);
    }

    [Fact(Timeout = 5_000)]
    public async Task RescheduleAndCancelInvalidateOldPositions()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        MqttZeroTimerNode rescheduled = CreateNode("rescheduled", now);
        MqttZeroTimerNode cancelled = CreateNode("cancelled", now);
        wheel.ScheduleOrRemove(
            rescheduled,
            now.AddMilliseconds(100).UtcTicks);
        wheel.ScheduleOrRemove(
            rescheduled,
            now.AddMilliseconds(300).UtcTicks);
        wheel.ScheduleOrRemove(
            cancelled,
            now.AddMilliseconds(200).UtcTicks);
        wheel.Cancel(cancelled);

        await TickOnShardAsync(
            shard,
            wheel,
            now + TimeSpan.FromMilliseconds(200),
            cancellationToken);

        Assert.Empty(fired);
        Assert.False(cancelled.IsScheduled);

        await TickOnShardAsync(
            shard,
            wheel,
            now + TimeSpan.FromMilliseconds(300),
            cancellationToken);

        Assert.Equal([rescheduled], fired);
        Assert.Equal(0, wheel.Count);
    }

    [Fact(Timeout = 5_000)]
    public async Task ReusedNodeIsScheduledAgainAfterItFires()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        MqttZeroTimerNode node = CreateNode("reused", now);
        wheel.ScheduleOrRemove(
            node,
            now.AddMilliseconds(100).UtcTicks);

        await TickOnShardAsync(
            shard,
            wheel,
            now + TimeSpan.FromMilliseconds(100),
            cancellationToken);
        wheel.ScheduleOrRemove(
            node,
            now.AddMilliseconds(200).UtcTicks);
        await TickOnShardAsync(
            shard,
            wheel,
            now + TimeSpan.FromMilliseconds(200),
            cancellationToken);

        Assert.Equal([node, node], fired);
        Assert.Equal(0, wheel.Count);
    }

    [Fact(Timeout = 5_000)]
    public async Task DistinctDueTicksFireInAscendingOrder()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        MqttZeroTimerNode third = CreateNode("third", now);
        MqttZeroTimerNode first = CreateNode("first", now);
        MqttZeroTimerNode second = CreateNode("second", now);
        wheel.ScheduleOrRemove(
            third,
            now.AddMilliseconds(300).UtcTicks);
        wheel.ScheduleOrRemove(
            first,
            now.AddMilliseconds(100).UtcTicks);
        wheel.ScheduleOrRemove(
            second,
            now.AddMilliseconds(200).UtcTicks);

        await TickOnShardAsync(
            shard,
            wheel,
            now + TimeSpan.FromMilliseconds(300),
            cancellationToken);

        Assert.Equal([first, second, third], fired);
    }

    [Fact(Timeout = 5_000)]
    public async Task MaximumExpiryDurationDoesNotOverflow()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            []);
        MqttZeroTimerNode node = CreateNode("maximum", now);
        long dueTicks = now.AddSeconds(uint.MaxValue).UtcTicks;

        wheel.ScheduleOrRemove(node, dueTicks);

        Assert.Equal(dueTicks, node.DueUtcTicks);
        Assert.Equal(3, node.Level);
        Assert.True(wheel.Count > 0);
        cancellationToken.ThrowIfCancellationRequested();
    }

    [Fact(Timeout = 5_000)]
    public async Task TickRejectsCallersOutsideTheOwningShard()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            []);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => wheel.TickAsync(now.UtcTicks, cancellationToken)
                .AsTask());
    }

    [Fact(Timeout = 5_000)]
    public async Task EmptyTickIsAllocationFreeAfterWarmup()
    {
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            []);
        for (int index = 0; index < 10; index++)
        {
            await TickOnShardAsync(
                shard,
                wheel,
                now + TimeSpan.FromTicks(
                    TickInterval.Ticks * index),
                TestContext.Current.CancellationToken);
        }

        long allocated = await shard.InvokeAsync(
            async cancellationToken =>
            {
                long before = GC.GetAllocatedBytesForCurrentThread();
                await wheel.TickAsync(
                    now.AddSeconds(1).UtcTicks,
                    cancellationToken).ConfigureAwait(false);
                return GC.GetAllocatedBytesForCurrentThread() - before;
            },
            TestContext.Current.CancellationToken);

        Assert.Equal(0, allocated);
    }

    [Fact(Timeout = 5_000)]
    public async Task TickerExecutesDueNodeOnTheOwningShard()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        TimeProvider timeProvider = TimeProvider.System;
        await using MqttZeroEventLoop shard = new(0, 64);
        TaskCompletionSource<bool> fired = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await using MqttZeroShardTimerWheel wheel = new(
            shard,
            timeProvider,
            TimeSpan.FromMilliseconds(10),
            (_, _) =>
            {
                fired.TrySetResult(shard.IsCurrent);
                return ValueTask.CompletedTask;
            },
            diagnostics: null);
        MqttZeroTimerNode node = CreateNode(
            "ticker",
            timeProvider.GetUtcNow());
        wheel.ScheduleOrRemove(
            node,
            timeProvider.GetUtcNow()
                .AddMilliseconds(50)
                .UtcTicks);

        wheel.Start();

        Assert.True(await fired.Task.WaitAsync(cancellationToken));
    }

    [Fact(Timeout = 5_000)]
    public async Task OverdueNodeScheduledAfterCurrentSlotWasVisitedFiresNextTick()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        await TickOnShardAsync(
            shard,
            wheel,
            now,
            cancellationToken);
        MqttZeroTimerNode overdue = CreateNode("overdue", now);
        wheel.ScheduleOrRemove(overdue, now.UtcTicks);

        await TickOnShardAsync(
            shard,
            wheel,
            now + TickInterval,
            cancellationToken);

        Assert.Equal([overdue], fired);
    }

    [Fact(Timeout = 10_000)]
    public async Task TenThousandNodesOnlyProcessTheReachedBucket()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        MqttZeroTimerNode[] nodes = new MqttZeroTimerNode[10_000];
        for (int index = 0; index < nodes.Length; index++)
        {
            MqttZeroTimerNode node = CreateNode(
                index.ToString(CultureInfo.InvariantCulture),
                now);
            nodes[index] = node;
            wheel.ScheduleOrRemove(
                node,
                now.AddTicks(
                    TickInterval.Ticks * (index + 1)).UtcTicks);
        }

        await TickOnShardAsync(
            shard,
            wheel,
            now + TickInterval,
            cancellationToken);

        Assert.Equal([nodes[0]], fired);
        Assert.Equal(9_999, wheel.Count);
    }

    [Fact(Timeout = 10_000)]
    public async Task StopClearsScheduledNodesAndIsIdempotent()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        MqttZeroTimerNode due = CreateNode("due", now);
        MqttZeroTimerNode later = CreateNode("later", now.AddMinutes(1));
        wheel.ScheduleOrRemove(due, now.UtcTicks);
        wheel.ScheduleOrRemove(later, now.AddMinutes(1).UtcTicks);
        Assert.Equal(2, wheel.Count);

        await wheel.StopAsync().AsTask().WaitAsync(
            TimeSpan.FromSeconds(5),
            cancellationToken);

        Assert.Equal(0, wheel.Count);
        Assert.Empty(fired);
        await wheel.StopAsync().AsTask().WaitAsync(
            TimeSpan.FromSeconds(5),
            cancellationToken);

        await TickOnShardAsync(shard, wheel, now, cancellationToken);

        Assert.Empty(fired);
        Assert.Equal(0, wheel.Count);
        await wheel.DisposeAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task StopCompletesWhileTicksAreScheduled()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        List<MqttZeroTimerNode> fired = [];
        MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            fired);
        MqttZeroTimerNode later = CreateNode("later", now.AddMinutes(1));
        wheel.ScheduleOrRemove(later, now.AddMinutes(1).UtcTicks);
        wheel.Start();
        await Task.Delay(
            TickInterval + TickInterval,
            cancellationToken);

        await wheel.StopAsync().AsTask().WaitAsync(
            TimeSpan.FromSeconds(5),
            cancellationToken);

        Assert.Equal(0, wheel.Count);
        Assert.Empty(fired);
        await wheel.DisposeAsync();
    }

    [Fact]
    public async Task ScheduleUpdateAndCancelAreAllocationFreeAfterNodeCreation()
    {
        DateTimeOffset now = new(2026, 11, 26, 8, 0, 0, TimeSpan.Zero);
        MqttZeroTestTimeProvider timeProvider = new(now);
        await using MqttZeroEventLoop shard = new(0, 64);
        await using MqttZeroShardTimerWheel wheel = CreateWheel(
            shard,
            timeProvider,
            []);
        MqttZeroTimerNode node = CreateNode("allocations", now);
        for (int index = 0; index < 32; index++)
        {
            wheel.ScheduleOrRemove(
                node,
                now.AddMinutes(1).UtcTicks);
            wheel.ScheduleOrRemove(
                node,
                now.AddMinutes(2).UtcTicks);
            wheel.Cancel(node);
        }

        long allocated = 0;
        for (int index = 0; index < 1_000; index++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            wheel.ScheduleOrRemove(
                node,
                now.AddMinutes(1).UtcTicks);
            wheel.ScheduleOrRemove(
                node,
                now.AddMinutes(2).UtcTicks);
            wheel.Cancel(node);
            allocated +=
                GC.GetAllocatedBytesForCurrentThread() - before;
        }

        Assert.Equal(0, allocated);
        await Task.CompletedTask;
    }

    private static MqttZeroShardTimerWheel CreateWheel(
        MqttZeroEventLoop shard,
        TimeProvider timeProvider,
        List<MqttZeroTimerNode> fired)
    {
        return new MqttZeroShardTimerWheel(
            shard,
            timeProvider,
            TickInterval,
            (node, _) =>
            {
                fired.Add(node);
                return ValueTask.CompletedTask;
            },
            diagnostics: null);
    }

    private static MqttZeroTimerNode CreateNode(
        string id,
        DateTimeOffset now)
    {
        return new MqttZeroTimerNode(
            MqttZeroDeadlineKind.Will,
            id,
            now);
    }

    private static async Task TickOnShardAsync(
        MqttZeroEventLoop shard,
        MqttZeroShardTimerWheel wheel,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await shard.InvokeAsync(
            async token =>
            {
                await wheel.TickAsync(
                    now.UtcTicks,
                    token).ConfigureAwait(false);
                return true;
            },
            cancellationToken).ConfigureAwait(false);
    }

    private sealed class MqttZeroTestTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow;

        public MqttZeroTestTimeProvider(DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public override DateTimeOffset GetUtcNow()
        {
            return _utcNow;
        }
    }
}
