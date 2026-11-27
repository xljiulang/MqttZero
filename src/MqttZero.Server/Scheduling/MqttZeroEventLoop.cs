using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace MqttZero.Server.Scheduling;

/// <summary>
/// 单个事件循环分片。
/// </summary>
internal sealed class MqttZeroEventLoop :
    IThreadPoolWorkItem,
    IAsyncDisposable
{
    private const int MaximumWorkItemsPerDrain = 1024;
    private static readonly long DrainBudgetTimestampTicks =
        Math.Max(1, Stopwatch.Frequency / 2000);
    private static readonly AsyncLocal<int?> CurrentShardIdState = new();

    private readonly ConcurrentQueue<MqttZeroShardWork> _queue = new();
    private readonly ConcurrentQueue<MqttZeroShardWork> _lowPriorityQueue =
        new();
    private readonly SemaphoreSlim _queueSpace;
    private readonly TaskCompletionSource _stopped = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly bool _trackPriorityDiagnostics;
    private long _completedCount;
    private long _enqueuedCount;
    private long _failedCount;
    private int _highPriorityQueuedCount;
    private int _lowPriorityQueuedCount;
    private long _lowPriorityWaitCount;
    private long _maxDrainTimestampTicks;
    private int _queuedCount;
    private int _scheduled;
    private int _stopping;

    /// <summary>
    /// 初始化 MqttZeroEventLoop 的新实例。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <param name="queueCapacity">队列容量。</param>
    /// <param name="trackPriorityDiagnostics">trackPriority诊断。</param>
    public MqttZeroEventLoop(
        int shardId,
        int queueCapacity,
        bool trackPriorityDiagnostics = true)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueCapacity);
        ShardId = shardId;
        _trackPriorityDiagnostics = trackPriorityDiagnostics;
        _queueSpace = new SemaphoreSlim(
            queueCapacity,
            queueCapacity);
    }

    /// <summary>
    /// 分片Id。
    /// </summary>
    public int ShardId { get; }

    /// <summary>
    /// IsCurrent 的值。
    /// </summary>
    public bool IsCurrent
    {
        get
        {
            return CurrentShardId == ShardId;
        }
    }

    /// <summary>
    /// CurrentShardId 的值。
    /// </summary>
    internal static int? CurrentShardId
    {
        get
        {
            return CurrentShardIdState.Value;
        }
    }

    /// <summary>
    /// 入队。
    /// </summary>
    /// <param name="work">工作项。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

        EnqueueCore(work);
        Schedule();
    }

    /// <summary>
    /// 尝试Enqueue。
    /// </summary>
    /// <param name="work">工作项。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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

        EnqueueCore(work);
        Schedule();
        return true;
    }

    /// <summary>
    /// 调用。
    /// </summary>
    /// <param name="operation">对应的 operation。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
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

    /// <summary>
    /// 调用。
    /// </summary>
    /// <param name="command">对应的 command。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<T> InvokeAsync<T>(
        MqttZeroShardCommand<T> command,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (IsCurrent)
        {
            return await command
                .ExecuteInlineAsync()
                .ConfigureAwait(false);
        }

        try
        {
            if (!TryEnqueue(command))
            {
                await EnqueueAsync(command, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            command.Fail(exception);
        }

        return await command.Completion.ConfigureAwait(false);
    }

    /// <summary>
    /// 获取快照。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroShardDiagnostics GetSnapshot()
    {
        double highPriorityOldestWorkAgeMilliseconds = 0;
        if (_trackPriorityDiagnostics &&
            _queue.TryPeek(out MqttZeroShardWork? highPriorityWork))
        {
            highPriorityOldestWorkAgeMilliseconds = Stopwatch.GetElapsedTime(
                highPriorityWork.EnqueuedTimestamp).TotalMilliseconds;
        }

        double lowPriorityOldestWorkAgeMilliseconds = 0;
        if (_trackPriorityDiagnostics &&
            _lowPriorityQueue.TryPeek(
            out MqttZeroShardWork? lowPriorityWork))
        {
            lowPriorityOldestWorkAgeMilliseconds = Stopwatch.GetElapsedTime(
                lowPriorityWork.EnqueuedTimestamp).TotalMilliseconds;
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
            Math.Max(
                highPriorityOldestWorkAgeMilliseconds,
                lowPriorityOldestWorkAgeMilliseconds),
            _trackPriorityDiagnostics
                ? Volatile.Read(ref _highPriorityQueuedCount)
                : 0,
            _trackPriorityDiagnostics
                ? Volatile.Read(ref _lowPriorityQueuedCount)
                : 0,
            highPriorityOldestWorkAgeMilliseconds,
            lowPriorityOldestWorkAgeMilliseconds,
            _trackPriorityDiagnostics
                ? Interlocked.Read(ref _lowPriorityWaitCount)
                : 0);
    }

    /// <summary>
    /// 停止。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) == 0)
        {
            Schedule();
        }

        return new ValueTask(_stopped.Task);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _queueSpace.Dispose();
    }

    /// <inheritdoc/>
    void IThreadPoolWorkItem.Execute()
    {
        _ = DrainAsync();
    }

    /// <summary>
    /// 排空。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    private async Task DrainAsync()
    {
        int previousShardId = CurrentShardIdState.Value ?? -1;
        CurrentShardIdState.Value = ShardId;
        long drainStarted = Stopwatch.GetTimestamp();
        int processedCount = 0;
        try
        {
            while (processedCount < MaximumWorkItemsPerDrain &&
                Stopwatch.GetElapsedTime(drainStarted) <
                    TimeSpan.FromTicks(DrainBudgetTimestampTicks))
            {
                if (!TryDequeueWork(out MqttZeroShardWork? work))
                {
                    break;
                }

                MqttZeroShardWork dequeuedWork = work;
                try
                {
                    await dequeuedWork.ExecuteAsync(
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
                    dequeuedWork.Release();
                    processedCount++;
                }
            }
        }
        finally
        {
            long drainElapsed = Stopwatch.GetTimestamp() - drainStarted;
            UpdateMaximumDrain(drainElapsed);
            CurrentShardIdState.Value =
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

    /// <summary>
    /// 调度。
    /// </summary>
    private void Schedule()
    {
        if (Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0)
        {
            ThreadPool.UnsafeQueueUserWorkItem(
                this,
                preferLocal: false);
        }
    }

    /// <summary>
    /// 入队。
    /// </summary>
    /// <param name="work">工作项。</param>
    private void EnqueueCore(MqttZeroShardWork work)
    {
        work.MarkEnqueued();
        if (work.IsLowPriority)
        {
            _lowPriorityQueue.Enqueue(work);
            if (_trackPriorityDiagnostics)
            {
                Interlocked.Increment(ref _lowPriorityQueuedCount);
            }
        }
        else
        {
            _queue.Enqueue(work);
            if (_trackPriorityDiagnostics)
            {
                Interlocked.Increment(ref _highPriorityQueuedCount);
            }
        }

        Interlocked.Increment(ref _queuedCount);
        Interlocked.Increment(ref _enqueuedCount);
    }

    /// <summary>
    /// 尝试Dequeue工作项。
    /// </summary>
    /// <param name="work">工作项。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private bool TryDequeueWork([NotNullWhen(true)] out MqttZeroShardWork? work)
    {
        if (_queue.TryDequeue(out work))
        {
            if (_trackPriorityDiagnostics)
            {
                Interlocked.Decrement(ref _highPriorityQueuedCount);
            }

            Interlocked.Decrement(ref _queuedCount);
            if (_trackPriorityDiagnostics &&
                !_lowPriorityQueue.IsEmpty)
            {
                Interlocked.Increment(ref _lowPriorityWaitCount);
            }

            return true;
        }

        if (_lowPriorityQueue.TryDequeue(out work))
        {
            if (_trackPriorityDiagnostics)
            {
                Interlocked.Decrement(ref _lowPriorityQueuedCount);
            }

            Interlocked.Decrement(ref _queuedCount);
            return true;
        }

        return false;
    }

    /// <summary>
    /// 抛出IfStopping。
    /// </summary>
    private void ThrowIfStopping()
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _stopping) != 0,
            this);
    }

    /// <summary>
    /// 更新最大排空。
    /// </summary>
    /// <param name="drainElapsed">排空Elapsed。</param>
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
