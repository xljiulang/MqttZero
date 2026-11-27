using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroShardCommandTests
{
    [Fact(Timeout = 10_000)]
    public async Task ReusedCommandCompletesAcrossShard()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        MqttZeroShardCommand<int> command = new();

        for (int expected = 1; expected <= 20; expected++)
        {
            command.Prepare(
                _ => ValueTask.FromResult(expected),
                TestContext.Current.CancellationToken);

            int actual = await shard.InvokeAsync(
                command,
                TestContext.Current.CancellationToken);

            Assert.Equal(expected, actual);
        }

        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task CommandPropagatesExceptionAndRemainsReusable()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        MqttZeroShardCommand<int> command = new();
        command.Prepare(
            _ => ValueTask.FromException<int>(
                new InvalidOperationException("expected")),
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await shard.InvokeAsync(
                command,
                TestContext.Current.CancellationToken));

        command.Prepare(
            _ => ValueTask.FromResult(42),
            TestContext.Current.CancellationToken);
        int result = await shard.InvokeAsync(
            command,
            TestContext.Current.CancellationToken);

        Assert.Equal(42, result);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task PreparedCommandCannotBePreparedAgain()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        MqttZeroShardCommand<int> command = new();
        command.Prepare(
            _ => ValueTask.FromResult(1),
            TestContext.Current.CancellationToken);

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(() =>
                command.Prepare(
                    _ => ValueTask.FromResult(2),
                    TestContext.Current.CancellationToken));

        Assert.Contains("already in use", exception.Message);
        Assert.Equal(
            1,
            await shard.InvokeAsync(
                command,
                TestContext.Current.CancellationToken));
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task SameShardCommandExecutesInline()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        MqttZeroShardCommand<int> command = new();
        TaskCompletionSource<int> completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    command.Prepare(
                        _ => ValueTask.FromResult(7),
                        cancellationToken);
                    completed.TrySetResult(
                        await shard.InvokeAsync(
                            command,
                            cancellationToken));
                }),
            TestContext.Current.CancellationToken);

        Assert.Equal(
            7,
            await completed.Task.WaitAsync(
                TestContext.Current.CancellationToken));
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task CancellationBeforeEnqueueLeavesCommandReusable()
    {
        await using MqttZeroEventLoop shard = new(0, 1);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    blockerStarted.TrySetResult();
                    await releaseBlocker.Task.WaitAsync(
                        cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await blockerStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        MqttZeroShardCommand<int> command = new();
        command.Prepare(
            _ => ValueTask.FromResult(42),
            TestContext.Current.CancellationToken);
        using CancellationTokenSource cancellation = new();
        ValueTask<int> cancelledInvocation = shard.InvokeAsync(
            command,
            cancellation.Token);
        Assert.False(cancelledInvocation.IsCompleted);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await cancelledInvocation);

        releaseBlocker.TrySetResult();
        await WaitForQueueSpaceAsync(shard);
        command.Prepare(
            _ => ValueTask.FromResult(43),
            TestContext.Current.CancellationToken);

        Assert.Equal(
            43,
            await shard.InvokeAsync(
                command,
                TestContext.Current.CancellationToken));
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task CancellationAfterEnqueueDoesNotDetachCaller()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        MqttZeroShardCommand<int> command = new();
        TaskCompletionSource operationStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseOperation = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource cancellation = new();
        command.Prepare(
            async _ =>
            {
                operationStarted.TrySetResult();
                await releaseOperation.Task;
                return 42;
            },
            cancellation.Token);
        Task<int> invocation = shard.InvokeAsync(
            command,
            cancellation.Token).AsTask();
        await operationStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        cancellation.Cancel();

        Assert.False(invocation.IsCompleted);
        releaseOperation.TrySetResult();
        Assert.Equal(
            42,
            await invocation.WaitAsync(
                TestContext.Current.CancellationToken));
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task PreparedCommandExecutionDoesNotAllocateAfterWarmup()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        MqttZeroShardCommand<int> command = new();
        for (int index = 0; index < 5; index++)
        {
            command.Prepare(
                _ => ValueTask.FromResult(42),
                TestContext.Current.CancellationToken);
            await command.ExecuteAsync(
                shard,
                CancellationToken.None);
            Assert.Equal(
                42,
                await command.Completion);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        int lastResult = 0;
        for (int index = 0; index < 10_000; index++)
        {
            command.Prepare(
                _ => ValueTask.FromResult(42),
                TestContext.Current.CancellationToken);
            await command.ExecuteAsync(
                shard,
                CancellationToken.None);
            lastResult = await command.Completion;
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(42, lastResult);
        Assert.InRange(allocated, 0, 64);
        await shard.StopAsync();
    }

    private static async Task WaitForQueueSpaceAsync(
        MqttZeroEventLoop shard)
    {
        long deadline = Environment.TickCount64 + 5_000;
        while (shard.GetSnapshot().QueuedCount != 0)
        {
            if (Environment.TickCount64 >= deadline)
            {
                throw new TimeoutException(
                    "The shard queue did not drain.");
            }

            await Task.Yield();
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
