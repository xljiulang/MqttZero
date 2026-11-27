using MqttZero.Memory;
using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

/// <summary>
/// 单个目标分片的扇出批次工作项。
/// </summary>
internal sealed class MqttZeroFanoutBatchWork : MqttZeroShardWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroFanoutBatchWork> Pool = new(MqttZeroBoundedObjectPool<MqttZeroFanoutBatchWork>.DefaultCapacity);

    private MqttZeroFanoutOperation? _operation;
    private int _shardId;

    /// <summary>
    /// 初始化 MqttZeroFanoutBatchWork 的新实例。
    /// </summary>
    private MqttZeroFanoutBatchWork()
    {
    }

    /// <summary>
    /// PoolCountForTest 的值。
    /// </summary>
    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
    }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="operation">对应的 operation。</param>
    /// <param name="shardId">分片Id。</param>
    /// <returns>对应的结果。</returns>
    internal static MqttZeroFanoutBatchWork Rent(
        MqttZeroFanoutOperation operation,
        int shardId)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        if (!Pool.TryRent(out MqttZeroFanoutBatchWork? work) ||
            work is null)
        {
            work = new MqttZeroFanoutBatchWork();
        }

        work._operation = operation;
        work._shardId = shardId;
        return work;
    }

    /// <summary>
    /// 执行。
    /// </summary>
    /// <param name="shard">分片。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 释放。
    /// </summary>
    public override void Release()
    {
        _operation = null;
        _shardId = 0;
        _ = Pool.Return(this);
    }
}
