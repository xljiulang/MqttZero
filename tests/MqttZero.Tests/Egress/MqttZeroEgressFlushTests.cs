using MqttZero.Memory;
using MqttZero.Server.Broker;
using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;
using System.IO.Pipelines;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressFlushTests
{
    [Fact(Timeout = 5_000)]
    public async Task SynchronousFlushIsRecorded()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics);
        MqttZeroSessionEgress? actorReference = null;
        actorReference = new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                Assert.True(actorReference!.BeginFlush(
                    ValueTask.FromResult(
                        new FlushResult(
                            isCanceled: false,
                            isCompleted: false))));
                return ValueTask.FromResult(false);
            },
            _ => { });
        await using MqttZeroSessionEgress actor = actorReference;

        actor.Signal();
        await WaitUntilAsync(
            () => diagnostics.GetSnapshot().EgressDrainCount == 1,
            TestContext.Current.CancellationToken);

        Assert.Equal(
            1,
            diagnostics.GetSnapshot().EgressSynchronousFlushCount);
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task FlushPendingDoesNotBlockOtherActorsOnSameShard()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource firstFlushStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource secondActorRan = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        MqttZeroSessionEgress? firstActor = null;
        firstActor = new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                bool synchronous = firstActor!.BeginFlush(
                    new ValueTask<FlushResult>(flushCompletion.Task),
                    encodedBytes: 4096);
                Assert.False(synchronous);
                firstFlushStarted.TrySetResult();
                return ValueTask.FromResult(false);
            },
            _ => { });
        MqttZeroSessionEgress first = firstActor;
        MqttZeroSessionEgress second = new(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                secondActorRan.TrySetResult();
                return ValueTask.FromResult(false);
            },
            _ => { });

        first.Signal();
        second.Signal();

        await firstFlushStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await secondActorRan.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();
        Assert.Equal(1, snapshot.CurrentEgressFlushPendingCount);
        Assert.Equal(4096, snapshot.CurrentEncodedBytesAwaitingFlush);
        Assert.Equal(
            MqttZeroSessionEgressState.FlushPending,
            first.State);

        flushCompletion.TrySetResult(new FlushResult(false, false));
        await WaitUntilAsync(
            () => first.State == MqttZeroSessionEgressState.Idle,
            TestContext.Current.CancellationToken);
        snapshot = diagnostics.GetSnapshot();
        Assert.Equal(0, snapshot.CurrentEgressFlushPendingCount);
        Assert.Equal(0, snapshot.CurrentEncodedBytesAwaitingFlush);
        await first.StopAsync();
        await second.StopAsync();
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task CompletedFlushWithoutWorkDoesNotResumeActor()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource resumed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int drainCount = 0;
        MqttZeroSessionEgress? actorReference = null;
        actorReference = new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                if (Interlocked.Increment(ref drainCount) == 1)
                {
                    Assert.False(actorReference!.BeginFlush(
                        new ValueTask<FlushResult>(flushCompletion.Task)));
                }
                else
                {
                    resumed.TrySetResult();
                }

                return ValueTask.FromResult(false);
            },
            _ => { });
        await using MqttZeroSessionEgress actor = actorReference;

        actor.Signal();
        await WaitUntilAsync(
            () => actor.State == MqttZeroSessionEgressState.FlushPending,
            TestContext.Current.CancellationToken);
        flushCompletion.TrySetResult(new FlushResult(false, false));
        await WaitUntilAsync(
            () => actor.State == MqttZeroSessionEgressState.Idle,
            TestContext.Current.CancellationToken);
        await Task.Delay(
            TimeSpan.FromMilliseconds(100),
            TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref drainCount));
        Assert.False(resumed.Task.IsCompleted);
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task SignalDuringDrainWaitsForPendingFlush()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource flushStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource secondDrainStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int drainCount = 0;
        MqttZeroSessionEgress? actorReference = null;
        actorReference = new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                if (Interlocked.Increment(ref drainCount) == 1)
                {
                    actorReference!.Signal();
                    Assert.False(actorReference.BeginFlush(
                        new ValueTask<FlushResult>(flushCompletion.Task)));
                    flushStarted.TrySetResult();
                }
                else
                {
                    secondDrainStarted.TrySetResult();
                }

                return ValueTask.FromResult(false);
            },
            _ => { });
        await using MqttZeroSessionEgress actor = actorReference;

        actor.Signal();
        await flushStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await Task.Delay(
            TimeSpan.FromMilliseconds(100),
            TestContext.Current.CancellationToken);
        bool drainedWhileFlushPending =
            Volatile.Read(ref drainCount) != 1;
        flushCompletion.TrySetResult(new FlushResult(false, false));
        await secondDrainStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await actor.StopAsync();
        Assert.False(drainedWhileFlushPending);
        Assert.Equal(2, Volatile.Read(ref drainCount));
    }

    [Fact(Timeout = 5_000)]
    public async Task ControlWaitsForPendingDataFlush()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource flushStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource controlRequestStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource controlWriteStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        using MqttZeroEgressControlFramePool framePool =
            new(bufferPool);
        MqttZeroSessionEgress? actorReference = null;
        actorReference = new MqttZeroSessionEgress(
            scheduler,
            (_, _) =>
            {
                controlWriteStarted.TrySetResult();
                return ValueTask.CompletedTask;
            },
            _ =>
            {
                if (actorReference!.State !=
                    MqttZeroSessionEgressState.FlushPending)
                {
                    Assert.False(actorReference.BeginFlush(
                        new ValueTask<FlushResult>(flushCompletion.Task)));
                    flushStarted.TrySetResult();
                }

                return ValueTask.FromResult(false);
            },
            _ => { });
        await using MqttZeroSessionEgress actor = actorReference;
        MqttZeroEgressControlFrame control = CreateFrame(framePool);

        try
        {
            actor.Signal();
            await flushStarted.Task.WaitAsync(
                TestContext.Current.CancellationToken);
            Task controlCompletion = shard.InvokeAsync(
                async cancellationToken =>
                {
                    controlRequestStarted.TrySetResult();
                    await actor.PostControlAsync(
                        control,
                        cancellationToken).ConfigureAwait(false);
                    return true;
                },
                TestContext.Current.CancellationToken).AsTask();
            await controlRequestStarted.Task.WaitAsync(
                TestContext.Current.CancellationToken);
            await Task.Delay(
                TimeSpan.FromMilliseconds(100),
                TestContext.Current.CancellationToken);
            bool wroteWhileFlushPending =
                controlWriteStarted.Task.IsCompleted;

            flushCompletion.TrySetResult(new FlushResult(false, false));
            await controlCompletion.WaitAsync(
                TestContext.Current.CancellationToken);
            await controlWriteStarted.Task.WaitAsync(
                TestContext.Current.CancellationToken);
            await actor.StopAsync();
            Assert.False(wroteWhileFlushPending);
        }
        finally
        {
            control.Release();
        }
    }

    [Fact(Timeout = 5_000)]
    public async Task SignalWhileFlushPendingResumesAfterCompletion()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource flushStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource resumed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int drainCount = 0;
        MqttZeroSessionEgress? actorReference = null;
        actorReference = new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                if (Interlocked.Increment(ref drainCount) == 1)
                {
                    Assert.False(actorReference!.BeginFlush(
                        new ValueTask<FlushResult>(flushCompletion.Task)));
                    flushStarted.TrySetResult();
                }
                else
                {
                    resumed.TrySetResult();
                }

                return ValueTask.FromResult(false);
            },
            _ => { });
        await using MqttZeroSessionEgress actor = actorReference;

        actor.Signal();
        await flushStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        actor.Signal();
        await Task.Delay(
            TimeSpan.FromMilliseconds(100),
            TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref drainCount));

        flushCompletion.TrySetResult(new FlushResult(false, false));
        await resumed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task FlushFailureIsReportedOnOwningShard()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> failureObserved = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        MqttZeroSessionEgress? actorReference = null;
        actorReference = new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                Assert.False(actorReference!.BeginFlush(
                    new ValueTask<FlushResult>(flushCompletion.Task)));
                return ValueTask.FromResult(false);
            },
            _ => failureObserved.TrySetResult(shard.IsCurrent));
        await using MqttZeroSessionEgress actor = actorReference;

        actor.Signal();
        await WaitUntilAsync(
            () => actor.State == MqttZeroSessionEgressState.FlushPending,
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new BlockingShardWork(
                blockerStarted,
                releaseBlocker),
            TestContext.Current.CancellationToken);
        await blockerStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        flushCompletion.TrySetResult(
            new FlushResult(
                isCanceled: false,
                isCompleted: true));
        await Task.Delay(
            TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        bool reportedWhileShardBlocked =
            failureObserved.Task.IsCompleted;
        releaseBlocker.TrySetResult();
        Assert.False(reportedWhileShardBlocked);
        Assert.True(await failureObserved.Task.WaitAsync(
            TestContext.Current.CancellationToken));
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task StopFailsQueuedControlAndWaitsForPendingFlush()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource flushStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        using MqttZeroEgressControlFramePool framePool =
            new(bufferPool);
        MqttZeroSessionEgress? actorReference = null;
        actorReference = new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                Assert.False(actorReference!.BeginFlush(
                    new ValueTask<FlushResult>(flushCompletion.Task)));
                flushStarted.TrySetResult();
                return ValueTask.FromResult(false);
            },
            _ => { });
        await using MqttZeroSessionEgress actor = actorReference;
        MqttZeroEgressControlFrame control = CreateFrame(framePool);

        actor.Signal();
        await flushStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shard.InvokeAsync(
            async cancellationToken =>
            {
                bool actorOwnsFrame = await actor.PostControlAsync(
                    control,
                    cancellationToken).ConfigureAwait(false);
                Assert.True(actorOwnsFrame);
                return true;
            },
            TestContext.Current.CancellationToken);

        Task stop = actor.StopAsync().AsTask();
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await control.WaitAsync().AsTask().WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken));
        Assert.False(stop.IsCompleted);
        flushCompletion.TrySetResult(new FlushResult(false, false));
        await stop.WaitAsync(TestContext.Current.CancellationToken);
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task StopCompletesWhenCalledFromOwningShard()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        await using MqttZeroSessionEgress actor = new(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ => ValueTask.FromResult(false),
            _ => { });

        bool completed = await shard.InvokeAsync(
            async cancellationToken =>
            {
                await actor.StopAsync().AsTask().WaitAsync(
                    cancellationToken).ConfigureAwait(false);
                return true;
            },
            TestContext.Current.CancellationToken);

        Assert.True(completed);
    }

    [Fact(Timeout = 5_000)]
    public async Task StopFromOwningShardResumesAfterPendingFlush()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource flushStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        MqttZeroSessionEgress? actorReference = null;
        actorReference = new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                Assert.False(actorReference!.BeginFlush(
                    new ValueTask<FlushResult>(flushCompletion.Task)));
                flushStarted.TrySetResult();
                return ValueTask.FromResult(false);
            },
            _ => { });
        await using MqttZeroSessionEgress actor = actorReference;

        actor.Signal();
        await flushStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Task<bool> stop = shard.InvokeAsync<bool>(
            async cancellationToken =>
            {
                await actor.StopAsync().AsTask().WaitAsync(
                    cancellationToken).ConfigureAwait(false);
                return true;
            },
            TestContext.Current.CancellationToken).AsTask();
        await Task.Delay(
            TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        Assert.False(stop.IsCompleted);

        flushCompletion.TrySetResult(new FlushResult(false, false));
        Assert.True(await stop.WaitAsync(
            TestContext.Current.CancellationToken));
    }

    private static MqttZeroEgressControlFrame CreateFrame(
        MqttZeroEgressControlFramePool framePool)
    {
        MqttZeroEgressControlFrame frame = framePool.Rent(1);
        Span<byte> span = frame.Writer.GetSpan(1);
        span[0] = 0;
        frame.Writer.Advance(1);
        return frame;
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

    private sealed class BlockingShardWork : MqttZeroShardWork
    {
        private readonly TaskCompletionSource _release;
        private readonly TaskCompletionSource _started;

        public BlockingShardWork(
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
