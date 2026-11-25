using MqttZero.Memory;
using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;
using System.IO.Pipelines;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressFlushTests
{
    [Fact(Timeout = 5_000)]
    public async Task FlushPendingDoesNotBlockOtherActorsOnSameShard()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource<FlushResult> flushCompletion = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource firstFlushStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource secondActorRan = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        MqttZeroEgressActor? firstActor = null;
        firstActor = new MqttZeroEgressActor(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                bool synchronous = firstActor!.BeginFlush(
                    new ValueTask<FlushResult>(flushCompletion.Task));
                Assert.False(synchronous);
                firstFlushStarted.TrySetResult();
                return ValueTask.FromResult(false);
            },
            _ => { });
        MqttZeroEgressActor first = firstActor;
        MqttZeroEgressActor second = new(
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
        Assert.Equal(
            MqttZeroEgressActorState.FlushPending,
            first.State);

        flushCompletion.TrySetResult(new FlushResult(false, false));
        await WaitUntilAsync(
            () => first.State == MqttZeroEgressActorState.Idle,
            TestContext.Current.CancellationToken);
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
        MqttZeroEgressActor? actorReference = null;
        actorReference = new MqttZeroEgressActor(
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
        await using MqttZeroEgressActor actor = actorReference;

        actor.Signal();
        await WaitUntilAsync(
            () => actor.State == MqttZeroEgressActorState.FlushPending,
            TestContext.Current.CancellationToken);
        flushCompletion.TrySetResult(new FlushResult(false, false));
        await WaitUntilAsync(
            () => actor.State == MqttZeroEgressActorState.Idle,
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
        MqttZeroEgressActor? actorReference = null;
        actorReference = new MqttZeroEgressActor(
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
        await using MqttZeroEgressActor actor = actorReference;

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
        MqttZeroEgressActor? actorReference = null;
        actorReference = new MqttZeroEgressActor(
            scheduler,
            (_, _) =>
            {
                controlWriteStarted.TrySetResult();
                return ValueTask.CompletedTask;
            },
            _ =>
            {
                if (actorReference!.State !=
                    MqttZeroEgressActorState.FlushPending)
                {
                    Assert.False(actorReference.BeginFlush(
                        new ValueTask<FlushResult>(flushCompletion.Task)));
                    flushStarted.TrySetResult();
                }

                return ValueTask.FromResult(false);
            },
            _ => { });
        await using MqttZeroEgressActor actor = actorReference;
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
        MqttZeroEgressActor? actorReference = null;
        actorReference = new MqttZeroEgressActor(
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
        await using MqttZeroEgressActor actor = actorReference;

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
}
