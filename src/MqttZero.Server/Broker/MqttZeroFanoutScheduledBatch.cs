using System.Collections.Concurrent;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutScheduledBatch
{
    private static readonly ConcurrentQueue<MqttZeroFanoutScheduledBatch> Pool = new();

    private MqttZeroFanoutOperation? _operation;
    private int _shardId;

    private MqttZeroFanoutScheduledBatch()
    {
    }

    internal MqttZeroFanoutOperation Operation
    {
        get
        {
            return _operation ??
                throw new InvalidOperationException(
                    "The fanout scheduled batch was not initialized.");
        }
    }

    internal int ShardId
    {
        get
        {
            return _shardId;
        }
    }

    internal static MqttZeroFanoutScheduledBatch Rent(
        MqttZeroFanoutOperation operation,
        int shardId)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        if (!Pool.TryDequeue(out MqttZeroFanoutScheduledBatch? batch))
        {
            batch = new MqttZeroFanoutScheduledBatch();
        }

        batch._operation = operation;
        batch._shardId = shardId;
        return batch;
    }

    internal void Release()
    {
        _operation = null;
        _shardId = 0;
        Pool.Enqueue(this);
    }
}
