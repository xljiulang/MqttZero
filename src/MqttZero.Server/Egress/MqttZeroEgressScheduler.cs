using MqttZero.Server.Broker;
using MqttZero.Server.Scheduling;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace MqttZero.Server.Egress;

/// <summary>
/// 把出站工作调度到会话分片。
/// </summary>
internal sealed class MqttZeroEgressScheduler : IAsyncDisposable
{
    private const int MaximumActorExecutionsPerPass = 64;
    private static readonly long PassBudgetTimestampTicks =
        Math.Max(1, Stopwatch.Frequency / 4000);

    private readonly MqttZeroFanoutDiagnostics? _diagnostics;
    private readonly ConcurrentQueue<MqttZeroSessionEgress> _ready = new();
    private readonly TaskCompletionSource _stopped = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _signalsDrained = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly MqttZeroEgressSchedulerWork _work;
    private int _activeSignals;
    private int _scheduled;
    private int _stopping;

    /// <summary>
    /// 初始化 MqttZeroEgressScheduler 的新实例。
    /// </summary>
    /// <param name="shard">分片。</param>
    /// <param name="diagnostics">诊断。</param>
    public MqttZeroEgressScheduler(
        MqttZeroEventLoop shard,
        MqttZeroFanoutDiagnostics? diagnostics)
    {
        Shard = shard ?? throw new ArgumentNullException(nameof(shard));
        _diagnostics = diagnostics;
        _work = new MqttZeroEgressSchedulerWork(this);
    }

    /// <summary>
    /// 分片。
    /// </summary>
    public MqttZeroEventLoop Shard { get; }

    /// <summary>
    /// 通知。
    /// </summary>
    /// <param name="egress">出站。</param>
    public void Signal(MqttZeroSessionEgress egress)
    {
        ArgumentNullException.ThrowIfNull(egress);
        Interlocked.Increment(ref _activeSignals);
        try
        {
            if (Volatile.Read(ref _stopping) != 0 ||
                !egress.TryMarkReady())
            {
                return;
            }

            _diagnostics?.RecordEgressScheduledActor();
            _ready.Enqueue(egress);
            Schedule();
        }
        finally
        {
            if (Interlocked.Decrement(ref _activeSignals) == 0 &&
                Volatile.Read(ref _stopping) != 0)
            {
                _signalsDrained.TrySetResult();
            }
        }
    }

    /// <summary>
    /// 通知And排空当前分片。
    /// </summary>
    /// <param name="egress">出站。</param>
    /// <returns>表示异步操作的任务。</returns>
    internal ValueTask SignalAndDrainCurrentShardAsync(
        MqttZeroSessionEgress egress)
    {
        ArgumentNullException.ThrowIfNull(egress);
        if (!Shard.IsCurrent)
        {
            throw new InvalidOperationException(
                "The egress scheduler must run on its owning shard.");
        }

        Signal(egress);
        return ExecuteAsync(CancellationToken.None);
    }

    /// <summary>
    /// 停止。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
        {
            await _stopped.Task.ConfigureAwait(false);
            return;
        }

        if (Volatile.Read(ref _activeSignals) == 0)
        {
            _signalsDrained.TrySetResult();
        }

        await _signalsDrained.Task.ConfigureAwait(false);
        if (_ready.IsEmpty)
        {
            _stopped.TrySetResult();
            return;
        }

        Schedule();
        await _stopped.Task.ConfigureAwait(false);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// 执行。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    internal async ValueTask ExecuteAsync(
        CancellationToken cancellationToken)
    {
        long started = Stopwatch.GetTimestamp();
        int actorCount = 0;
        try
        {
            while (actorCount < MaximumActorExecutionsPerPass &&
                Stopwatch.GetElapsedTime(started) <
                    TimeSpan.FromTicks(PassBudgetTimestampTicks) &&
                _ready.TryDequeue(out MqttZeroSessionEgress? egress))
            {
                egress.MarkExecuting();
                _diagnostics?.RecordEgressDrain();
                try
                {
                    await egress.ExecuteAsync(cancellationToken)
                        .ConfigureAwait(false);
                }
                finally
                {
                    egress.OnExecutionReleased();
                }

                actorCount++;
            }
        }
        finally
        {
            _diagnostics?.RecordEgressSchedulerPass(
                actorCount,
                Stopwatch.GetTimestamp() - started);
        }
    }

    /// <summary>
    /// On工作项Released。
    /// </summary>
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
            Volatile.Read(ref _activeSignals) == 0)
        {
            _stopped.TrySetResult();
        }
    }

    /// <summary>
    /// 调度。
    /// </summary>
    private void Schedule()
    {
        if (Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0 &&
            !Shard.TryEnqueue(_work))
        {
            _ = ScheduleWorkAsync();
        }
    }

    /// <summary>
    /// 调度工作项。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    private async Task ScheduleWorkAsync()
    {
        try
        {
            await Shard.EnqueueAsync(
                _work,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            Volatile.Write(ref _scheduled, 0);
            _stopped.TrySetResult();
        }
    }
}
