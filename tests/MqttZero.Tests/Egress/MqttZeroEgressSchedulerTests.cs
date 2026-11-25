using MqttZero.Server.Broker;
using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressSchedulerTests
{
    [Fact(Timeout = 5_000)]
    public async Task SignalsForSameActorAreCoalesced()
    {
        await using MqttZeroEventLoop shard = new(0, 64);
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int drainCount = 0;
        await using MqttZeroEgressActor actor = CreateActor(
            scheduler,
            _ =>
            {
                Interlocked.Increment(ref drainCount);
                return ValueTask.FromResult(false);
            });

        await BlockShardAsync(
            shard,
            blockerStarted,
            releaseBlocker,
            TestContext.Current.CancellationToken);
        for (int index = 0; index < 100; index++)
        {
            actor.Signal();
        }

        releaseBlocker.TrySetResult();
        await WaitUntilAsync(
            () => Volatile.Read(ref drainCount) == 1,
            TestContext.Current.CancellationToken);
        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();
        Assert.Equal(1, snapshot.EgressScheduledActorCount);
        Assert.Equal(1, snapshot.EgressSchedulerPassCount);
        Assert.Equal(1, snapshot.EgressScheduledActorPassCount);
        Assert.Equal(1, snapshot.EgressDrainCount);
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task OneSchedulerPassDrainsMultipleActors()
    {
        await using MqttZeroEventLoop shard = new(0, 64);
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int drainCount = 0;
        MqttZeroEgressActor first = CreateActor(
            scheduler,
            _ =>
            {
                Interlocked.Increment(ref drainCount);
                return ValueTask.FromResult(false);
            });
        MqttZeroEgressActor second = CreateActor(
            scheduler,
            _ =>
            {
                Interlocked.Increment(ref drainCount);
                return ValueTask.FromResult(false);
            });
        MqttZeroEgressActor third = CreateActor(
            scheduler,
            _ =>
            {
                Interlocked.Increment(ref drainCount);
                return ValueTask.FromResult(false);
            });

        try
        {
            await BlockShardAsync(
                shard,
                blockerStarted,
                releaseBlocker,
                TestContext.Current.CancellationToken);
            first.Signal();
            second.Signal();
            third.Signal();
            releaseBlocker.TrySetResult();

            await WaitUntilAsync(
                () => Volatile.Read(ref drainCount) == 3,
                TestContext.Current.CancellationToken);
            MqttZeroFanoutDiagnosticsSnapshot snapshot =
                diagnostics.GetSnapshot();
            Assert.Equal(3, snapshot.EgressScheduledActorCount);
            Assert.Equal(1, snapshot.EgressSchedulerPassCount);
            Assert.Equal(3, snapshot.EgressScheduledActorPassCount);
        }
        finally
        {
            await first.StopAsync();
            await second.StopAsync();
            await third.StopAsync();
            await first.DisposeAsync();
            await second.DisposeAsync();
            await third.DisposeAsync();
        }
    }

    [Fact(Timeout = 5_000)]
    public async Task SignalDuringExecutionSchedulesOneFollowUp()
    {
        await using MqttZeroEventLoop shard = new(0, 64);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        int drainCount = 0;
        MqttZeroEgressActor? actor = null;
        actor = CreateActor(
            scheduler,
            _ =>
            {
                int current = Interlocked.Increment(ref drainCount);
                if (current == 1)
                {
                    actor!.Signal();
                }

                return ValueTask.FromResult(false);
            });

        try
        {
            actor.Signal();
            await WaitUntilAsync(
                () => Volatile.Read(ref drainCount) == 2,
                TestContext.Current.CancellationToken);
            await Task.Delay(
                TimeSpan.FromMilliseconds(20),
                TestContext.Current.CancellationToken);
            Assert.Equal(2, Volatile.Read(ref drainCount));
        }
        finally
        {
            await actor.StopAsync();
            await actor.DisposeAsync();
        }
    }

    private static MqttZeroEgressActor CreateActor(
        MqttZeroEgressScheduler scheduler,
        Func<CancellationToken, ValueTask<bool>> drainData)
    {
        return new MqttZeroEgressActor(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            drainData,
            _ => { });
    }

    private static async Task BlockShardAsync(
        MqttZeroEventLoop shard,
        TaskCompletionSource started,
        TaskCompletionSource release,
        CancellationToken cancellationToken)
    {
        await shard.EnqueueAsync(
            new BlockerWork(started, release),
            cancellationToken);
        await started.Task.WaitAsync(cancellationToken);
    }

    private static async Task WaitUntilAsync(
        Func<bool> predicate,
        CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(2));
        while (!predicate())
        {
            await Task.Delay(
                TimeSpan.FromMilliseconds(10),
                timeout.Token);
        }
    }

    private sealed class BlockerWork : MqttZeroShardWork
    {
        private readonly TaskCompletionSource _release;
        private readonly TaskCompletionSource _started;

        public BlockerWork(
            TaskCompletionSource started,
            TaskCompletionSource release)
        {
            _started = started;
            _release = release;
        }

        public override async ValueTask ExecuteAsync(
            MqttZeroEventLoop shard,
            CancellationToken cancellationToken)
        {
            _started.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
        }
    }
}
