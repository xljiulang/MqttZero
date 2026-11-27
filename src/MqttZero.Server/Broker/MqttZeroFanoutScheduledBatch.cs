using MqttZero.Memory;

namespace MqttZero.Server.Broker;

/// <summary>
/// 已调度到目标分片的扇出批次。
/// </summary>
internal sealed class MqttZeroFanoutScheduledBatch
{
    private static readonly MqttZeroBoundedObjectPool<
        MqttZeroFanoutScheduledBatch> Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroFanoutScheduledBatch>.
                DefaultCapacity);

    private MqttZeroFanoutOperation? _operation;
    private int _shardId;

    /// <summary>
    /// 初始化 MqttZeroFanoutScheduledBatch 的新实例。
    /// </summary>
    private MqttZeroFanoutScheduledBatch()
    {
    }

    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
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

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="operation">对应的 operation。</param>
    /// <param name="shardId">分片Id。</param>
    /// <returns>对应的结果。</returns>
    internal static MqttZeroFanoutScheduledBatch Rent(
        MqttZeroFanoutOperation operation,
        int shardId)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        if (!Pool.TryRent(out MqttZeroFanoutScheduledBatch? batch) ||
            batch is null)
        {
            batch = new MqttZeroFanoutScheduledBatch();
        }

        batch._operation = operation;
        batch._shardId = shardId;
        return batch;
    }

    /// <summary>
    /// 释放。
    /// </summary>
    internal void Release()
    {
        _operation = null;
        _shardId = 0;
        _ = Pool.Return(this);
    }
}
