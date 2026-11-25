using MqttZero.Server.Scheduling;
using System.Collections.Concurrent;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutBatchWork : MqttZeroShardWork
{
    private static readonly ConcurrentQueue<MqttZeroFanoutBatchWork> Pool =
        new();

    private MqttZeroFanoutOperation? _operation;
    private int _shardId;

    private MqttZeroFanoutBatchWork()
    {
    }

    internal static MqttZeroFanoutBatchWork Rent(
        MqttZeroFanoutOperation operation,
        int shardId)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        if (!Pool.TryDequeue(out MqttZeroFanoutBatchWork? work))
        {
            work = new MqttZeroFanoutBatchWork();
        }

        work._operation = operation;
        work._shardId = shardId;
        return work;
    }

    public override async ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        MqttZeroFanoutOperation operation = _operation ??
            throw new InvalidOperationException(
                "The fanout batch work was not initialized.");
        try
        {
            await operation.ExecuteBatchAsync(
                _shardId,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            operation.Fail(exception);
            throw;
        }
        finally
        {
            operation.CompleteBatch();
        }
    }

    public override void Release()
    {
        _operation = null;
        _shardId = 0;
        Pool.Enqueue(this);
    }
}
