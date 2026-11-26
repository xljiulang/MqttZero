using MqttZero.Server.Broker;
using MqttZero.Server.Egress;

namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroShardSet : IAsyncDisposable
{
    private readonly MqttZeroEventLoop[] _shards;
    private readonly MqttZeroEgressScheduler[] _egressSchedulers;
    private readonly MqttZeroFanoutScheduler[] _fanoutSchedulers;
    private readonly MqttZeroSessionShardMap _sessionShardMap;
    private readonly MqttZeroShardTimerWheel[] _timerWheels;
    private int _disposed;
    private int _stopped;

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

    public int Count
    {
        get
        {
            return _shards.Length;
        }
    }

    public MqttZeroEventLoop this[int shardId]
    {
        get
        {
            return _shards[shardId];
        }
    }

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

    public MqttZeroEventLoop GetSessionShard(string clientId)
    {
        int shardId = _sessionShardMap.GetShardId(clientId);
        return _shards[shardId];
    }

    public MqttZeroEgressScheduler GetEgressScheduler(int shardId)
    {
        return _egressSchedulers[shardId];
    }

    public MqttZeroFanoutScheduler GetFanoutScheduler(int shardId)
    {
        return _fanoutSchedulers[shardId];
    }

    public MqttZeroShardTimerWheel GetTimerWheel(int shardId)
    {
        return _timerWheels[shardId];
    }

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

    private static ValueTask IgnoreTimerAsync(
        MqttZeroTimerNode node,
        CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
