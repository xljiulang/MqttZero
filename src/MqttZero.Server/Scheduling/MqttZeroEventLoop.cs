using System.Collections.Concurrent;
using System.Diagnostics;

namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroEventLoop :
    IThreadPoolWorkItem,
    IAsyncDisposable
{
    private const int MaximumWorkItemsPerDrain = 1024;
    private static readonly long DrainBudgetTimestampTicks =
        Math.Max(1, Stopwatch.Frequency / 2000);
    private static readonly AsyncLocal<int?> CurrentShardId = new();

    private readonly ConcurrentQueue<MqttZeroShardWork> _queue = new();
    private readonly ConcurrentQueue<MqttZeroShardWork> _lowPriorityQueue =
        new();
    private readonly SemaphoreSlim _queueSpace;
    private readonly TaskCompletionSource _stopped = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private long _completedCount;
    private long _enqueuedCount;
    private long _failedCount;
    private long _maxDrainTimestampTicks;
    private int _queuedCount;
    private int _scheduled;
    private int _stopping;

    public MqttZeroEventLoop(
        int shardId,
        int queueCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueCapacity);
        ShardId = shardId;
        _queueSpace = new SemaphoreSlim(
            queueCapacity,
            queueCapacity);
    }

    public int ShardId { get; }

    public bool IsCurrent
    {
        get
        {
            return CurrentShardId.Value == ShardId;
        }
    }

    public async ValueTask EnqueueAsync(
        MqttZeroShardWork work,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(work);
        ThrowIfStopping();
        await _queueSpace.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (Volatile.Read(ref _stopping) != 0)
        {
            _queueSpace.Release();
            throw new ObjectDisposedException(nameof(MqttZeroEventLoop));
        }

        work.MarkEnqueued();
        if (work.IsLowPriority)
        {
            _lowPriorityQueue.Enqueue(work);
        }
        else
        {
            _queue.Enqueue(work);
        }
        Interlocked.Increment(ref _queuedCount);
        Interlocked.Increment(ref _enqueuedCount);
        Schedule();
    }

    public bool TryEnqueue(MqttZeroShardWork work)
    {
        ArgumentNullException.ThrowIfNull(work);
        if (Volatile.Read(ref _stopping) != 0 ||
            !_queueSpace.Wait(0))
        {
            return false;
        }

        if (Volatile.Read(ref _stopping) != 0)
        {
            _queueSpace.Release();
            return false;
        }

        work.MarkEnqueued();
        if (work.IsLowPriority)
        {
            _lowPriorityQueue.Enqueue(work);
        }
        else
        {
            _queue.Enqueue(work);
        }

        Interlocked.Increment(ref _queuedCount);
        Interlocked.Increment(ref _enqueuedCount);
        Schedule();
        return true;
    }

    public async ValueTask<T> InvokeAsync<T>(
        Func<CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (IsCurrent)
        {
            return await operation(cancellationToken).ConfigureAwait(false);
        }

        MqttZeroShardInvocation<T> invocation = new(operation);
        await EnqueueAsync(invocation, cancellationToken)
            .ConfigureAwait(false);
        return await invocation.Task
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    public MqttZeroShardDiagnostics GetSnapshot()
    {
        double oldestWorkAgeMilliseconds = 0;
        if (_queue.TryPeek(out MqttZeroShardWork? oldestWork) ||
            _lowPriorityQueue.TryPeek(out oldestWork))
        {
            oldestWorkAgeMilliseconds = Stopwatch.GetElapsedTime(
                oldestWork.EnqueuedTimestamp).TotalMilliseconds;
        }

        return new MqttZeroShardDiagnostics(
            ShardId,
            Interlocked.Read(ref _enqueuedCount),
            Interlocked.Read(ref _completedCount),
            Interlocked.Read(ref _failedCount),
            Volatile.Read(ref _queuedCount),
            Volatile.Read(ref _scheduled) != 0,
            Volatile.Read(ref _maxDrainTimestampTicks) *
                1000d /
                Stopwatch.Frequency,
            oldestWorkAgeMilliseconds);
    }

    public ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) == 0)
        {
            Schedule();
        }

        return new ValueTask(_stopped.Task);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _queueSpace.Dispose();
    }

    void IThreadPoolWorkItem.Execute()
    {
        _ = DrainAsync();
    }

    private async Task DrainAsync()
    {
        int previousShardId = CurrentShardId.Value ?? -1;
        CurrentShardId.Value = ShardId;
        long drainStarted = Stopwatch.GetTimestamp();
        int processedCount = 0;
        try
        {
            while (processedCount < MaximumWorkItemsPerDrain &&
                Stopwatch.GetElapsedTime(drainStarted) <
                    TimeSpan.FromTicks(DrainBudgetTimestampTicks))
            {
                if (!_queue.TryDequeue(out MqttZeroShardWork? work) &&
                    !_lowPriorityQueue.TryDequeue(out work))
                {
                    break;
                }

                Interlocked.Decrement(ref _queuedCount);
                try
                {
                    await work.ExecuteAsync(
                        this,
                        CancellationToken.None).ConfigureAwait(false);
                    Interlocked.Increment(ref _completedCount);
                }
                catch (Exception)
                {
                    Interlocked.Increment(ref _failedCount);
                }
                finally
                {
                    _queueSpace.Release();
                    work.Release();
                    processedCount++;
                }
            }
        }
        finally
        {
            long drainElapsed = Stopwatch.GetTimestamp() - drainStarted;
            UpdateMaximumDrain(drainElapsed);
            CurrentShardId.Value =
                previousShardId >= 0 ? previousShardId : null;
            Volatile.Write(ref _scheduled, 0);
            Thread.MemoryBarrier();
            if (!_queue.IsEmpty || !_lowPriorityQueue.IsEmpty)
            {
                Schedule();
            }
            else if (Volatile.Read(ref _stopping) != 0)
            {
                _stopped.TrySetResult();
            }
        }
    }

    private void Schedule()
    {
        if (Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0)
        {
            ThreadPool.UnsafeQueueUserWorkItem(
                this,
                preferLocal: false);
        }
    }

    private void ThrowIfStopping()
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _stopping) != 0,
            this);
    }

    private void UpdateMaximumDrain(long drainElapsed)
    {
        long current;
        do
        {
            current = Volatile.Read(ref _maxDrainTimestampTicks);
            if (drainElapsed <= current)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(
            ref _maxDrainTimestampTicks,
            drainElapsed,
            current) != current);
    }
}
