namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroShardSet : IAsyncDisposable
{
    private readonly MqttZeroEventLoop[] _shards;
    private readonly MqttZeroSessionShardMap _sessionShardMap;
    private int _disposed;
    private int _stopped;

    public MqttZeroShardSet(
        int shardCount,
        int queueCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueCapacity);
        _sessionShardMap = new MqttZeroSessionShardMap(shardCount);
        _shards = new MqttZeroEventLoop[shardCount];
        for (int index = 0; index < _shards.Length; index++)
        {
            _shards[index] = new MqttZeroEventLoop(
                index,
                queueCapacity);
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

    public async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

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
        for (int index = 0; index < _shards.Length; index++)
        {
            await _shards[index].DisposeAsync().ConfigureAwait(false);
        }
    }
}
