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
    public async Task CompletedFlushSignalsActorToResume()
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
        await resumed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await actor.StopAsync();
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
