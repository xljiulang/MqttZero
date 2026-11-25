using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroEventLoopTests
{
    [Fact(Timeout = 5_000)]
    public async Task ProcessesWorkInOrderWithSingleActiveDrain()
    {
        await using MqttZeroEventLoop shard = new(3, 128);
        List<int> order = [];
        int active = 0;
        int maximumActive = 0;
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        for (int index = 0; index < 100; index++)
        {
            int value = index;
            await shard.EnqueueAsync(
                new DelegateShardWork(
                    async cancellationToken =>
                    {
                        int current = Interlocked.Increment(ref active);
                        int observed;
                        do
                        {
                            observed = Volatile.Read(ref maximumActive);
                        }
                        while (current > observed &&
                            Interlocked.CompareExchange(
                                ref maximumActive,
                                current,
                                observed) != observed);

                        order.Add(value);
                        await Task.Yield();
                        Interlocked.Decrement(ref active);
                        if (order.Count == 100)
                        {
                            completed.TrySetResult();
                        }
                    }),
                TestContext.Current.CancellationToken);
        }

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(0, 100), order);
        Assert.Equal(1, maximumActive);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task AsyncWorkBlocksFollowingWorkUntilCompletion()
    {
        await using MqttZeroEventLoop shard = new(0, 16);
        TaskCompletionSource firstStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource secondCompleted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        bool secondStarted = false;

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    secondStarted = true;
                    secondCompleted.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        await firstStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await Task.Delay(
            TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        Assert.False(secondStarted);

        releaseFirst.TrySetResult();
        await secondCompleted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Assert.True(secondStarted);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task EnqueueAppliesBackpressureWhenQueueIsFull()
    {
        await using MqttZeroEventLoop shard = new(0, 1);
        TaskCompletionSource releaseFirst = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource bothQueued = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    await releaseFirst.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        Task secondEnqueue = shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    bothQueued.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken).AsTask();

        await Task.Delay(
            TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        Assert.False(secondEnqueue.IsCompleted);

        releaseFirst.TrySetResult();
        await secondEnqueue.WaitAsync(
            TestContext.Current.CancellationToken);
        await bothQueued.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task WorkFailureDoesNotStopFollowingWork()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ => throw new InvalidOperationException("expected")),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    completed.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        MqttZeroShardDiagnostics snapshot = shard.GetSnapshot();
        Assert.Equal(1, snapshot.FailedCount);
        Assert.Equal(1, snapshot.CompletedCount);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task StopDrainsQueuedWorkAndRejectsNewWork()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        TaskCompletionSource release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource secondCompleted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    await release.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    secondCompleted.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        Task stop = shard.StopAsync().AsTask();
        Assert.False(stop.IsCompleted);
        release.TrySetResult();
        await stop.WaitAsync(TestContext.Current.CancellationToken);
        await secondCompleted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await shard.EnqueueAsync(
                new DelegateShardWork(_ => ValueTask.CompletedTask),
                TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = 5_000)]
    public async Task IsCurrentIsTrueOnlyInsideOwningShard()
    {
        await using MqttZeroEventLoop shard = new(1, 8);
        TaskCompletionSource<bool> current = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    current.TrySetResult(shard.IsCurrent);
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        Assert.True(await current.Task.WaitAsync(
            TestContext.Current.CancellationToken));
        Assert.False(shard.IsCurrent);
        await shard.StopAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task ProcessesBudgetedBacklogAcrossMultipleDrains()
    {
        await using MqttZeroEventLoop shard = new(0, 2048);
        const int workCount = 2048;
        int completedCount = 0;
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        for (int index = 0; index < workCount; index++)
        {
            await shard.EnqueueAsync(
                new DelegateShardWork(
                    _ =>
                    {
                        Thread.SpinWait(32);
                        if (Interlocked.Increment(ref completedCount) ==
                            workCount)
                        {
                            completed.TrySetResult();
                        }

                        return ValueTask.CompletedTask;
                    }),
                TestContext.Current.CancellationToken);
        }

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(workCount, completedCount);
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
