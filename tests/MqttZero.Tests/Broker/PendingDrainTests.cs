using MqttZero.Server.Broker;

namespace MqttZero.Tests.Broker;

public sealed class PendingDrainTests
{
    [Fact]
    public async Task CompletingTheOnlyWorkItemCompletesTheEpisode()
    {
        MqttZeroPendingDrain drain = new();
        TaskCompletionSource token = drain.Begin();
        Assert.Equal(1, drain.Count);
        Assert.False(token.Task.IsCompleted);

        drain.Complete(token);

        Assert.Equal(0, drain.Count);
        await token.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task StaleTokenCannotCompleteTheNextEpisode()
    {
        MqttZeroPendingDrain drain = new();
        TaskCompletionSource first = drain.Begin();
        drain.Complete(first);

        TaskCompletionSource second = drain.Begin();
        Assert.NotSame(first, second);

        drain.Complete(first);

        Assert.Equal(1, drain.Count);
        Assert.False(second.Task.IsCompleted);

        drain.Complete(second);
        Assert.Equal(0, drain.Count);
        await second.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task DrainWaitsForEveryWorkItemInTheEpisode()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttZeroPendingDrain drain = new();
        TaskCompletionSource first = drain.Begin();
        TaskCompletionSource second = drain.Begin();
        Assert.Same(first, second);
        Assert.Equal(2, drain.Count);

        Task wait = drain.WaitAsync().AsTask();
        Assert.False(wait.IsCompleted);

        drain.Complete(first);
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        Assert.False(wait.IsCompleted);

        drain.Complete(second);
        await wait.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
        Assert.Equal(0, drain.Count);
    }

    [Fact]
    public void CountIsVisibleToTheCallerThatRegisteredTheWork()
    {
        MqttZeroPendingDrain drain = new();
        Assert.Equal(0, drain.Count);

        TaskCompletionSource first = drain.Begin();
        Assert.Equal(1, drain.Count);

        TaskCompletionSource second = drain.Begin();
        Assert.Equal(2, drain.Count);

        drain.Complete(first);
        Assert.Equal(1, drain.Count);

        drain.Complete(second);
        Assert.Equal(0, drain.Count);
    }

    [Fact]
    public async Task WaitReturnsImmediatelyWhenIdle()
    {
        MqttZeroPendingDrain drain = new();

        await drain.WaitAsync().AsTask().WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task RepeatedEpisodesDrainIndependently()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttZeroPendingDrain drain = new();
        TaskCompletionSource? previous = null;
        for (int iteration = 0; iteration < 1000; iteration++)
        {
            TaskCompletionSource token = drain.Begin();
            Assert.NotSame(previous, token);
            Assert.Equal(1, drain.Count);

            if (previous != null)
            {
                drain.Complete(previous);
                Assert.Equal(1, drain.Count);
            }

            drain.Complete(token);
            Assert.Equal(0, drain.Count);
            await drain.WaitAsync().AsTask().WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            previous = token;
        }
    }

    [Fact]
    public async Task ConcurrentBeginAndCompleteKeepTheCountBalanced()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttZeroPendingDrain drain = new();
        int workers = 8;
        int iterations = 500;
        Task[] tasks = new Task[workers];
        for (int worker = 0; worker < workers; worker++)
        {
            tasks[worker] = Task.Run(
                () =>
                {
                    for (int iteration = 0; iteration < iterations; iteration++)
                    {
                        TaskCompletionSource token = drain.Begin();
                        drain.Complete(token);
                    }
                },
                cancellationToken);
        }

        await Task.WhenAll(tasks).WaitAsync(
            TimeSpan.FromSeconds(30),
            cancellationToken);
        Assert.Equal(0, drain.Count);
        await drain.WaitAsync().AsTask().WaitAsync(
            TimeSpan.FromSeconds(5),
            cancellationToken);
    }
}
