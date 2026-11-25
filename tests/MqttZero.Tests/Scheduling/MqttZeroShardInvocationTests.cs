using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroShardInvocationTests
{
    [Fact(Timeout = 5_000)]
    public async Task SameShardInvocationRunsInline()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    int value = await shard.InvokeAsync(
                        _ => ValueTask.FromResult(42),
                        cancellationToken);
                    Assert.Equal(42, value);
                    completed.TrySetResult();
                }),
            TestContext.Current.CancellationToken);

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task CrossShardInvocationCompletesAndPropagatesExceptions()
    {
        await using MqttZeroEventLoop shard = new(0, 8);

        int value = await shard.InvokeAsync(
            _ => ValueTask.FromResult(17),
            TestContext.Current.CancellationToken);
        Assert.Equal(17, value);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await shard.InvokeAsync<int>(
                _ => throw new InvalidOperationException("expected"),
                TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = 5_000)]
    public async Task CancelledWaitDoesNotBreakShard()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        TaskCompletionSource release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource started = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    started.TrySetResult();
                    await release.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await started.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        using CancellationTokenSource cancellation = new(
            TimeSpan.FromMilliseconds(50));

        bool cancelled = false;
        try
        {
            _ = await shard.InvokeAsync(
                _ => ValueTask.FromResult(1),
                cancellation.Token);
        }
        catch (OperationCanceledException)
        {
            cancelled = true;
        }

        Assert.True(cancelled);
        release.TrySetResult();
        int result = await shard.InvokeAsync(
            _ => ValueTask.FromResult(2),
            TestContext.Current.CancellationToken);
        Assert.Equal(2, result);
        await shard.StopAsync();
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
