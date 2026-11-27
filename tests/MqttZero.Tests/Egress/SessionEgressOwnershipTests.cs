using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;
using System.IO.Pipelines;

namespace MqttZero.Tests.Egress;

public sealed class SessionEgressOwnershipTests
{
    [Fact(Timeout = 10_000)]
    public async Task SessionEgressOwnsControlDataAndStopLifecycle()
    {
        await using MqttZeroEventLoop shard = new(0, 16);
        await using MqttZeroEgressScheduler scheduler = new(shard, null);
        TaskCompletionSource drainStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int writeCount = 0;
        Exception? failure = null;
        await using MqttZeroSessionEgress egress = new(
            scheduler,
            (_, _) =>
            {
                Interlocked.Increment(ref writeCount);
                return ValueTask.CompletedTask;
            },
            _ =>
            {
                drainStarted.TrySetResult();
                return ValueTask.FromResult(false);
            },
            exception => failure = exception);

        egress.Signal();
        await drainStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await egress.StopAsync();

        Assert.Equal(0, writeCount);
        Assert.Null(failure);
        Assert.Equal(MqttZeroSessionEgressState.Stopped, egress.State);
    }

    [Fact(Timeout = 10_000)]
    public async Task SessionEgressFlushAndStopAreIdempotent()
    {
        await using MqttZeroEventLoop shard = new(0, 16);
        await using MqttZeroEgressScheduler scheduler = new(shard, null);
        await using MqttZeroSessionEgress egress = new(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ => ValueTask.FromResult(false),
            _ => { });

        await egress.StopAsync().AsTask().WaitAsync(
            TestContext.Current.CancellationToken);
        await egress.StopAsync().AsTask().WaitAsync(
            TestContext.Current.CancellationToken);

        Assert.True(egress.IsStopping);
        Assert.Equal(MqttZeroSessionEgressState.Stopped, egress.State);
        egress.Signal();
    }
}
