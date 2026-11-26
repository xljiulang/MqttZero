using MqttZero.Server.Broker;
using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroShardOwnershipTests
{
    [Fact(Timeout = 5_000)]
    public async Task EnsureCurrentPreservesOwnershipAcrossAsyncContinuation()
    {
        await using MqttZeroEventLoop shard = new(2, 8);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async _ =>
                {
                    MqttZeroShardOwnership.EnsureCurrent(
                        2,
                        "before-yield");
                    await Task.Yield();
                    MqttZeroShardOwnership.EnsureCurrent(
                        2,
                        "after-yield");
                    completed.TrySetResult();
                }),
            TestContext.Current.CancellationToken);

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task EnsureCurrentRejectsNonOwningThreads()
    {
        Assert.Throws<InvalidOperationException>(
            () => MqttZeroShardOwnership.EnsureCurrent(
                1,
                "non-shard"));

        await Task.CompletedTask;
    }

    [Fact(Timeout = 5_000)]
    public async Task EnsureCurrentRejectsWrongShardAndRecordsDiagnostic()
    {
        await using MqttZeroEventLoop shard = new(1, 8);
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    Assert.Throws<InvalidOperationException>(
                        () => MqttZeroShardOwnership.EnsureCurrent(
                            0,
                            "wrong-shard",
                            diagnostics));
                    completed.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(
            1,
            diagnostics.GetSnapshot().ShardOwnershipViolationCount);
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
