using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;
using System.IO.Pipelines;
using System.Threading.Tasks.Sources;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressFlushAllocationTests
{
    [Fact(Timeout = 5_000)]
    public async Task AsynchronousBeginFlushAvoidsManagedTaskAllocation()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        await using MqttZeroSessionEgress actor = new(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ => ValueTask.FromResult(false),
            _ => { });
        ManualFlushSource source = new();
        for (int index = 0; index < 32; index++)
        {
            Assert.False(actor.BeginFlush(source.CreateValueTask()));
            source.Complete();
        }

        long beginAllocated = 0;
        long completionAllocated = 0;
        for (int index = 0; index < 1_000; index++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            Assert.False(actor.BeginFlush(source.CreateValueTask()));
            beginAllocated +=
                GC.GetAllocatedBytesForCurrentThread() - before;

            before = GC.GetAllocatedBytesForCurrentThread();
            source.Complete();
            completionAllocated +=
                GC.GetAllocatedBytesForCurrentThread() - before;
        }

        Assert.InRange(beginAllocated, 0, 64_000);
        Assert.Equal(0, completionAllocated);
        await actor.StopAsync().AsTask().WaitAsync(
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public void ReusableFlushOperationSurvivesSynchronousFailure()
    {
        IOException expected = new("flush failed");
        Exception? observed = null;
        MqttZeroEgressFlushOperation operation = new(
            exception => observed = exception);

#pragma warning disable CA2012
        operation.Start(
            ValueTask.FromException<FlushResult>(expected));
#pragma warning restore CA2012
        Assert.Same(expected, observed);

        ManualFlushSource source = new();
#pragma warning disable CA2012
        operation.Start(source.CreateValueTask());
#pragma warning restore CA2012
        source.Complete();
        Assert.Null(observed);
    }

    private sealed class ManualFlushSource :
        IValueTaskSource,
        IValueTaskSource<FlushResult>
    {
        private Action? _continuation;
        private bool _completed;
        private short _version;

        public ValueTask<FlushResult> CreateValueTask()
        {
            return new ValueTask<FlushResult>(this, _version);
        }

        public void Complete()
        {
            _completed = true;
            Action? continuation = Interlocked.Exchange(
                ref _continuation,
                null);
            continuation?.Invoke();
        }

        public FlushResult GetResult(short token)
        {
            ValidateToken(token);
            if (!_completed)
            {
                throw new InvalidOperationException(
                    "The manual flush has not completed.");
            }

            _completed = false;
            _version++;
            return new FlushResult(false, false);
        }

        void IValueTaskSource.GetResult(short token)
        {
            _ = GetResult(token);
        }

        public ValueTaskSourceStatus GetStatus(short token)
        {
            ValidateToken(token);
            return _completed
                ? ValueTaskSourceStatus.Succeeded
                : ValueTaskSourceStatus.Pending;
        }

        public void OnCompleted(
            Action<object?> continuation,
            object? state,
            short token,
            ValueTaskSourceOnCompletedFlags flags)
        {
            ValidateToken(token);
            if (_completed)
            {
                continuation(state);
                return;
            }

            if (state is Action action)
            {
                _continuation = action;
                return;
            }

            throw new InvalidOperationException(
                "The flush continuation state was not an Action.");
        }

        public void UnsafeOnCompleted(
            Action continuation,
            short token)
        {
            ValidateToken(token);
            if (_completed)
            {
                continuation();
                return;
            }

            _continuation = continuation;
        }

        private void ValidateToken(short token)
        {
            if (token != _version)
            {
                throw new InvalidOperationException(
                    "The manual flush token is stale.");
            }
        }
    }
}
