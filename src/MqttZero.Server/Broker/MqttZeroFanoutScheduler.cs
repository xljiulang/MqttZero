using MqttZero.Server.Scheduling;
using System.Collections.Concurrent;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutScheduler : IAsyncDisposable
{
    private const int MaximumEntriesPerPass = 64;

    private readonly SemaphoreSlim _capacity;
    private readonly ConcurrentQueue<MqttZeroFanoutScheduledBatch> _ready = new();
    private readonly MqttZeroEventLoop _shard;
    private readonly TaskCompletionSource _signalsDrained = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _stopped = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly MqttZeroFanoutSchedulerWork _work;
    private int _activeEnqueues;
    private int _scheduled;
    private int _stopping;
    private long _enqueuedCount;

    internal MqttZeroFanoutScheduler(
        MqttZeroEventLoop shard,
        int queueCapacity,
        MqttZeroFanoutDiagnostics? diagnostics)
    {
        _shard = shard ?? throw new ArgumentNullException(nameof(shard));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueCapacity);
        _ = diagnostics;
        _capacity = new SemaphoreSlim(queueCapacity, queueCapacity);
        _work = new MqttZeroFanoutSchedulerWork(this);
    }

    internal long EnqueuedCount
    {
        get
        {
            return Interlocked.Read(ref _enqueuedCount);
        }
    }

    internal ValueTask EnqueueAsync(
        MqttZeroFanoutOperation operation,
        int shardId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        return EnqueueCoreAsync(
            operation,
            shardId,
            cancellationToken);
    }

    internal async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
        {
            await _stopped.Task.ConfigureAwait(false);
            return;
        }

        if (Volatile.Read(ref _activeEnqueues) == 0)
        {
            _signalsDrained.TrySetResult();
        }

        await _signalsDrained.Task.ConfigureAwait(false);
        if (_ready.IsEmpty &&
            Volatile.Read(ref _scheduled) == 0)
        {
            _stopped.TrySetResult();
            return;
        }

        Schedule();
        await _stopped.Task.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _capacity.Dispose();
    }

    internal async ValueTask ExecuteAsync(
        CancellationToken cancellationToken)
    {
        int count = 0;
        while (count < MaximumEntriesPerPass &&
            _ready.TryDequeue(out MqttZeroFanoutScheduledBatch? batch))
        {
            try
            {
                await batch.Operation.ExecuteBatchAsync(
                    batch.ShardId,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                batch.Operation.Fail(exception);
            }
            finally
            {
                try
                {
                    batch.Operation.CompleteBatch();
                }
                finally
                {
                    batch.Release();
                    _capacity.Release();
                    count++;
                }
            }
        }
    }

    internal void OnWorkReleased()
    {
        Volatile.Write(ref _scheduled, 0);
        Thread.MemoryBarrier();
        if (!_ready.IsEmpty)
        {
            Schedule();
            return;
        }

        if (Volatile.Read(ref _stopping) != 0 &&
            Volatile.Read(ref _activeEnqueues) == 0)
        {
            _stopped.TrySetResult();
        }
    }

    private async ValueTask EnqueueCoreAsync(
        MqttZeroFanoutOperation operation,
        int shardId,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _activeEnqueues);
        try
        {
            ThrowIfStopping();
            await _capacity.WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            if (Volatile.Read(ref _stopping) != 0)
            {
                _capacity.Release();
                throw new ObjectDisposedException(
                    nameof(MqttZeroFanoutScheduler));
            }

            MqttZeroFanoutScheduledBatch batch =
                MqttZeroFanoutScheduledBatch.Rent(operation, shardId);
            Interlocked.Increment(ref _enqueuedCount);
            _ready.Enqueue(batch);
            Schedule();
        }
        finally
        {
            if (Interlocked.Decrement(ref _activeEnqueues) == 0 &&
                Volatile.Read(ref _stopping) != 0)
            {
                _signalsDrained.TrySetResult();
            }
        }
    }

    private void Schedule()
    {
        if (Volatile.Read(ref _stopping) != 0 &&
            _ready.IsEmpty)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0 &&
            !_shard.TryEnqueue(_work))
        {
            _ = ScheduleWorkAsync();
        }
    }

    private async Task ScheduleWorkAsync()
    {
        try
        {
            await _shard.EnqueueAsync(
                _work,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Volatile.Write(ref _scheduled, 0);
            FailPending(exception);
            _stopped.TrySetResult();
        }
    }

    private void FailPending(Exception exception)
    {
        while (_ready.TryDequeue(out MqttZeroFanoutScheduledBatch? batch))
        {
            try
            {
                batch.Operation.Fail(exception);
            }
            finally
            {
                batch.Operation.CompleteBatch();
                batch.Release();
                _capacity.Release();
            }
        }
    }

    private void ThrowIfStopping()
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _stopping) != 0,
            nameof(MqttZeroFanoutScheduler));
    }
}
