using MqttZero.Memory;
using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressActorTests
{
    [Fact(Timeout = 5_000)]
    public async Task SignalYieldsBeforeFirstDataDrain()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource drainStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int drainCount = 0;
        List<string> order = [];
        await using MqttZeroEgressActor actor = CreateActor(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ =>
            {
                order.Add("drain");
                Interlocked.Increment(ref drainCount);
                drainStarted.TrySetResult();
                return ValueTask.FromResult(false);
            },
            _ => { });

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    blockerStarted.TrySetResult();
                    await releaseBlocker.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await blockerStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        actor.Signal();
        await Task.Delay(
            TimeSpan.FromMilliseconds(20),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    order.Add("batch");
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);
        releaseBlocker.TrySetResult();
        await drainStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        await WaitUntilAsync(
            () => Volatile.Read(ref drainCount) == 1,
            TestContext.Current.CancellationToken);
        Assert.Equal(["batch", "drain"], order);
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task MultipleSignalsCoalesceIntoOneActiveDrain()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource firstDrainStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirstDrain = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int drainCount = 0;
        await using MqttZeroEgressActor actor = CreateActor(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            async cancellationToken =>
            {
                int current = Interlocked.Increment(ref drainCount);
                if (current == 1)
                {
                    firstDrainStarted.TrySetResult();
                    await releaseFirstDrain.Task.WaitAsync(cancellationToken);
                }

                return false;
            },
            _ => { });

        actor.Signal();
        actor.Signal();
        actor.Signal();
        await firstDrainStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref drainCount));
        actor.Signal();

        releaseFirstDrain.TrySetResult();
        await WaitUntilAsync(
            () => Volatile.Read(ref drainCount) == 2,
            TestContext.Current.CancellationToken);
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task ControlFramesAreWrittenBeforeDataDrain()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        List<string> sequence = [];
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        using MqttZeroEgressControlFramePool framePool =
            new(bufferPool);
        await using MqttZeroEgressActor actor = CreateActor(
            scheduler,
            (frames, _) =>
            {
                sequence.Add("control");
                return ValueTask.CompletedTask;
            },
            _ =>
            {
                sequence.Add("data");
                return ValueTask.FromResult(false);
            },
            _ => { });
        MqttZeroEgressControlFrame control = CreateFrame(framePool);

        Task controlCompletion = actor.PostControlAsync(
            control,
            TestContext.Current.CancellationToken).AsTask();
        actor.Signal();
        try
        {
            await controlCompletion.WaitAsync(
                TestContext.Current.CancellationToken);
            await WaitUntilAsync(
                () => sequence.Count == 2,
                TestContext.Current.CancellationToken);
            Assert.Equal(["control", "data"], sequence);
        }
        finally
        {
            control.Release();
            await actor.StopAsync();
        }
    }

    [Fact(Timeout = 5_000)]
    public async Task StopRejectsSignalsAndFailsWaitingControls()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        TaskCompletionSource controlWriteStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseControlWrite = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        using MqttZeroEgressControlFramePool framePool =
            new(bufferPool);
        await using MqttZeroEgressActor actor = CreateActor(
            scheduler,
            async (_, cancellationToken) =>
            {
                controlWriteStarted.TrySetResult();
                await releaseControlWrite.Task.WaitAsync(cancellationToken);
            },
            _ => ValueTask.FromResult(false),
            _ => { });
        MqttZeroEgressControlFrame control = CreateFrame(framePool);
        Task completion = actor.PostControlAsync(
            control,
            TestContext.Current.CancellationToken).AsTask();
        await controlWriteStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        Task stop = actor.StopAsync().AsTask();
        releaseControlWrite.TrySetResult();
        await stop.WaitAsync(TestContext.Current.CancellationToken);

        try
        {
            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await completion.WaitAsync(
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken));
        }
        finally
        {
            control.Release();
        }

        MqttZeroEgressControlFrame rejected = CreateFrame(framePool);
        try
        {
            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await actor.PostControlAsync(
                    rejected,
                    TestContext.Current.CancellationToken));
        }
        finally
        {
            rejected.Release();
        }
    }

    private static MqttZeroEgressActor CreateActor(
        MqttZeroEgressScheduler scheduler,
        Func<
            IReadOnlyList<MqttZeroEgressControlFrame>,
            CancellationToken,
            ValueTask> writeControls,
        Func<CancellationToken, ValueTask<bool>> drainData,
        Action<Exception> faulted)
    {
        return new MqttZeroEgressActor(
            scheduler,
            writeControls,
            drainData,
            faulted);
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

    private sealed class DelegateShardWork : MqttZeroShardWork
    {
        private readonly Func<CancellationToken, ValueTask> _execute;

        public DelegateShardWork(
            Func<CancellationToken, ValueTask> execute)
        {
            _execute = execute;
        }

        public override ValueTask ExecuteAsync(
            MqttZeroEventLoop shard,
            CancellationToken cancellationToken)
        {
            return _execute(cancellationToken);
        }
    }
}
