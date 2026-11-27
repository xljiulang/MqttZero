using MqttZero.Server.Broker;
using MqttZero.Server.Egress;

namespace MqttZero.Server.Scheduling;

/// <summary>
/// 全部分片及其调度器的集合。
/// </summary>
internal sealed class MqttZeroShardSet : IAsyncDisposable
{
    private readonly MqttZeroEventLoop[] _shards;
    private readonly MqttZeroEgressScheduler[] _egressSchedulers;
    private readonly MqttZeroFanoutScheduler[] _fanoutSchedulers;
    private readonly MqttZeroSessionShardMap _sessionShardMap;
    private readonly MqttZeroShardTimerWheel[] _timerWheels;
    private int _disposed;
    private int _stopped;

    /// <summary>
    /// 初始化 MqttZeroShardSet 的新实例。
    /// </summary>
    /// <param name="shardCount">分片数量。</param>
    /// <param name="queueCapacity">队列容量。</param>
    /// <param name="fanoutDiagnostics">fanout诊断。</param>
    /// <param name="timeProvider">时间Provider。</param>
    /// <param name="timerTickInterval">定时器Tick间隔。</param>
    /// <param name="timerCallback">定时器回调。</param>
    public MqttZeroShardSet(
        int shardCount,
        int queueCapacity,
        MqttZeroFanoutDiagnostics? fanoutDiagnostics = null,
        TimeProvider? timeProvider = null,
        TimeSpan? timerTickInterval = null,
        Func<
            MqttZeroTimerNode,
            CancellationToken,
            ValueTask>? timerCallback = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueCapacity);
        _sessionShardMap = new MqttZeroSessionShardMap(shardCount);
        _shards = new MqttZeroEventLoop[shardCount];
        _egressSchedulers =
            new MqttZeroEgressScheduler[shardCount];
        _fanoutSchedulers =
            new MqttZeroFanoutScheduler[shardCount];
        _timerWheels =
            new MqttZeroShardTimerWheel[shardCount];
        TimeProvider resolvedTimeProvider =
            timeProvider ?? TimeProvider.System;
        TimeSpan resolvedTimerTickInterval =
            timerTickInterval ?? TimeSpan.FromMilliseconds(500);
        Func<
            MqttZeroTimerNode,
            CancellationToken,
            ValueTask> resolvedTimerCallback =
            timerCallback ?? IgnoreTimerAsync;
        for (int index = 0; index < _shards.Length; index++)
        {
            _shards[index] = new MqttZeroEventLoop(
                index,
                queueCapacity,
                fanoutDiagnostics?.Enabled == true);
            _egressSchedulers[index] = new MqttZeroEgressScheduler(
                _shards[index],
                fanoutDiagnostics);
            _fanoutSchedulers[index] = new MqttZeroFanoutScheduler(
                _shards[index],
                queueCapacity,
                fanoutDiagnostics);
            _timerWheels[index] = new MqttZeroShardTimerWheel(
                _shards[index],
                resolvedTimeProvider,
                resolvedTimerTickInterval,
                resolvedTimerCallback,
                fanoutDiagnostics);
            _timerWheels[index].Start();
        }
    }

    /// <summary>
    /// Count 的值。
    /// </summary>
    public int Count
    {
        get
        {
            return _shards.Length;
        }
    }

    /// <summary>
    /// 按索引获取元素。
    /// </summary>
    public MqttZeroEventLoop this[int shardId]
    {
        get
        {
            return _shards[shardId];
        }
    }

    /// <summary>
    /// 获取快照。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroShardSetSnapshot GetSnapshot()
    {
        MqttZeroShardDiagnostics[] snapshots =
            new MqttZeroShardDiagnostics[_shards.Length];
        for (int index = 0; index < _shards.Length; index++)
        {
            snapshots[index] = _shards[index].GetSnapshot();
        }

        return new MqttZeroShardSetSnapshot(snapshots);
    }

    /// <summary>
    /// 获取会话分片。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroEventLoop GetSessionShard(string clientId)
    {
        int shardId = _sessionShardMap.GetShardId(clientId);
        return _shards[shardId];
    }

    /// <summary>
    /// 获取出站调度器。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroEgressScheduler GetEgressScheduler(int shardId)
    {
        return _egressSchedulers[shardId];
    }

    /// <summary>
    /// 获取Fanout调度器。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroFanoutScheduler GetFanoutScheduler(int shardId)
    {
        return _fanoutSchedulers[shardId];
    }

    /// <summary>
    /// 获取定时器时间轮。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroShardTimerWheel GetTimerWheel(int shardId)
    {
        return _timerWheels[shardId];
    }

    /// <summary>
    /// PendingTimerCount 的值。
    /// </summary>
    public int PendingTimerCount
    {
        get
        {
            int count = 0;
            for (int index = 0; index < _timerWheels.Length; index++)
            {
                count += _timerWheels[index].Count;
            }

            return count;
        }
    }

    /// <summary>
    /// 停止。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        await StopTimersAsync().ConfigureAwait(false);
        Task[] fanoutSchedulerStopTasks =
            new Task[_fanoutSchedulers.Length];
        for (int index = 0;
            index < _fanoutSchedulers.Length;
            index++)
        {
            fanoutSchedulerStopTasks[index] =
                _fanoutSchedulers[index].StopAsync().AsTask();
        }

        await Task.WhenAll(fanoutSchedulerStopTasks).ConfigureAwait(false);
        Task[] schedulerStopTasks =
            new Task[_egressSchedulers.Length];
        for (int index = 0; index < _egressSchedulers.Length; index++)
        {
            schedulerStopTasks[index] =
                _egressSchedulers[index].StopAsync().AsTask();
        }

        await Task.WhenAll(schedulerStopTasks).ConfigureAwait(false);
        Task[] stopTasks = new Task[_shards.Length];
        for (int index = 0; index < _shards.Length; index++)
        {
            stopTasks[index] = _shards[index].StopAsync().AsTask();
        }

        await Task.WhenAll(stopTasks).ConfigureAwait(false);
    }

    /// <summary>
    /// 停止Timers。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    internal async ValueTask StopTimersAsync()
    {
        Task[] timerStopTasks =
            new Task[_timerWheels.Length];
        for (int index = 0; index < _timerWheels.Length; index++)
        {
            timerStopTasks[index] =
                _timerWheels[index].StopAsync().AsTask();
        }

        await Task.WhenAll(timerStopTasks).ConfigureAwait(false);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
        for (int index = 0; index < _timerWheels.Length; index++)
        {
            await _timerWheels[index]
                .DisposeAsync()
                .ConfigureAwait(false);
        }

        for (int index = 0;
            index < _egressSchedulers.Length;
            index++)
        {
            await _egressSchedulers[index]
                .DisposeAsync()
                .ConfigureAwait(false);
        }

        for (int index = 0;
            index < _fanoutSchedulers.Length;
            index++)
        {
            await _fanoutSchedulers[index]
                .DisposeAsync()
                .ConfigureAwait(false);
        }

        for (int index = 0; index < _shards.Length; index++)
        {
            await _shards[index].DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Ignore定时器。
    /// </summary>
    /// <param name="node">节点。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private static ValueTask IgnoreTimerAsync(
        MqttZeroTimerNode node,
        CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
