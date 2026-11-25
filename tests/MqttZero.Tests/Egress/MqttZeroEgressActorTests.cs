using MqttZero.Memory;
using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressActorTests
{
    [Fact(Timeout = 5_000)]
    public async Task MultipleSignalsCoalesceIntoOneActiveDrain()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        TaskCompletionSource firstDrainStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirstDrain = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int drainCount = 0;
        await using MqttZeroEgressActor actor = CreateActor(
            shard,
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
        List<string> sequence = [];
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        await using MqttZeroEgressActor actor = CreateActor(
            shard,
            (frames, _) =>
            {
                sequence.Add("control");
                foreach (MqttZeroEgressControlFrame frame in frames)
                {
                    frame.Complete();
                }

                return ValueTask.CompletedTask;
            },
            _ =>
            {
                sequence.Add("data");
                return ValueTask.FromResult(false);
            },
            _ => { });
        MqttZeroEgressControlFrame control = CreateFrame(bufferPool);

        Task controlCompletion = actor.PostControlAsync(
            control,
            TestContext.Current.CancellationToken).AsTask();
        actor.Signal();
        await controlCompletion.WaitAsync(
            TestContext.Current.CancellationToken);

        Assert.Equal(["control", "data"], sequence);
        await actor.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task StopRejectsSignalsAndFailsWaitingControls()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        TaskCompletionSource controlWriteStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseControlWrite = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        await using MqttZeroEgressActor actor = CreateActor(
            shard,
            async (_, cancellationToken) =>
            {
                controlWriteStarted.TrySetResult();
                await releaseControlWrite.Task.WaitAsync(cancellationToken);
            },
            _ => ValueTask.FromResult(false),
            _ => { });
        MqttZeroEgressControlFrame control = CreateFrame(bufferPool);
        Task completion = actor.PostControlAsync(
            control,
            TestContext.Current.CancellationToken).AsTask();
        await controlWriteStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        Task stop = actor.StopAsync().AsTask();
        releaseControlWrite.TrySetResult();
        await stop.WaitAsync(TestContext.Current.CancellationToken);

        Assert.True(completion.IsCompleted);
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await actor.PostControlAsync(
                CreateFrame(bufferPool),
                TestContext.Current.CancellationToken));
    }

    private static MqttZeroEgressActor CreateActor(
        MqttZeroEventLoop shard,
        Func<
            IReadOnlyList<MqttZeroEgressControlFrame>,
            CancellationToken,
            ValueTask> writeControls,
        Func<CancellationToken, ValueTask<bool>> drainData,
        Action<Exception> faulted)
    {
        return new MqttZeroEgressActor(
            shard,
            writeControls,
            drainData,
            faulted);
    }

    private static MqttZeroEgressControlFrame CreateFrame(
        MqttZeroBufferPool bufferPool)
    {
        MqttZeroEgressControlFrame frame = new(bufferPool, 1);
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
