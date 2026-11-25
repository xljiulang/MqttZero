using MqttZero.Server.Broker;
using MqttZero.Server.Egress;

namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroShardSet : IAsyncDisposable
{
    private readonly MqttZeroEventLoop[] _shards;
    private readonly MqttZeroEgressScheduler[] _egressSchedulers;
    private readonly MqttZeroFanoutScheduler[] _fanoutSchedulers;
    private readonly MqttZeroSessionShardMap _sessionShardMap;
    private int _disposed;
    private int _stopped;

    public MqttZeroShardSet(
        int shardCount,
        int queueCapacity,
        MqttZeroFanoutDiagnostics? fanoutDiagnostics = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueCapacity);
        _sessionShardMap = new MqttZeroSessionShardMap(shardCount);
        _shards = new MqttZeroEventLoop[shardCount];
        _egressSchedulers =
            new MqttZeroEgressScheduler[shardCount];
        _fanoutSchedulers =
            new MqttZeroFanoutScheduler[shardCount];
        for (int index = 0; index < _shards.Length; index++)
        {
            _shards[index] = new MqttZeroEventLoop(
                index,
                queueCapacity);
            _egressSchedulers[index] = new MqttZeroEgressScheduler(
                _shards[index],
                fanoutDiagnostics);
            _fanoutSchedulers[index] = new MqttZeroFanoutScheduler(
                _shards[index],
                queueCapacity,
                fanoutDiagnostics);
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

    public async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        Task[] schedulerStopTasks =
            new Task[_egressSchedulers.Length];
        for (int index = 0; index < _egressSchedulers.Length; index++)
        {
            schedulerStopTasks[index] =
                _egressSchedulers[index].StopAsync().AsTask();
        }

        await Task.WhenAll(schedulerStopTasks).ConfigureAwait(false);
        Task[] fanoutSchedulerStopTasks =
            new Task[_fanoutSchedulers.Length];
        for (int index = 0; index < _fanoutSchedulers.Length; index++)
        {
            fanoutSchedulerStopTasks[index] =
                _fanoutSchedulers[index].StopAsync().AsTask();
        }

        await Task.WhenAll(fanoutSchedulerStopTasks).ConfigureAwait(false);
        Task[] stopTasks = new Task[_shards.Length];
        for (int index = 0; index < _shards.Length; index++)
        {
            stopTasks[index] = _shards[index].StopAsync().AsTask();
        }

        await Task.WhenAll(stopTasks).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
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
}
