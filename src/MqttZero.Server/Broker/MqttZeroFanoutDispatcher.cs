using MqttZero.Memory;
using MqttZero.Server.Clients;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using System.Diagnostics;
using System.Runtime.ExceptionServices;

namespace MqttZero.Server.Broker;

/// <summary>
/// 按目标分片分批下发扇出投递。
/// </summary>
internal sealed class MqttZeroFanoutDispatcher
{
    private readonly Func<MqttZeroPlannedDelivery, MqttZeroMessageLease, MqttZeroClientConnection?, CancellationToken, ValueTask> _deliver;
    private readonly MqttZeroFanoutDiagnostics? _diagnostics;
    private readonly Action<Exception> _faulted;
    private readonly MqttZeroShardSet _shards;

    /// <summary>
    /// 初始化 MqttZeroFanoutDispatcher 的新实例。
    /// </summary>
    /// <param name="shards">分片。</param>
    /// <param name="deliver">对应的 deliver。</param>
    /// <param name="faulted">故障回调。</param>
    /// <param name="diagnostics">诊断。</param>
    public MqttZeroFanoutDispatcher(
        MqttZeroShardSet shards,
        Func<
            MqttZeroPlannedDelivery,
            MqttZeroMessageLease,
            MqttZeroClientConnection?,
            CancellationToken,
            ValueTask> deliver,
        Action<Exception> faulted,
        MqttZeroFanoutDiagnostics? diagnostics = null)
    {
        _shards = shards ??
            throw new ArgumentNullException(nameof(shards));
        _deliver = deliver ??
            throw new ArgumentNullException(nameof(deliver));
        _faulted = faulted ??
            throw new ArgumentNullException(nameof(faulted));
        _diagnostics = diagnostics;
    }

    /// <summary>
    /// 分发。
    /// </summary>
    /// <param name="plan">计划。</param>
    /// <param name="buffer">缓冲区。</param>
    /// <param name="lease">租约。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="publisherShardId">publisher分片Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask DispatchAsync(
        MqttZeroRoutePlan plan,
        MqttZeroRoutePlanBuffer buffer,
        MqttZeroMessageLease lease,
        MqttZeroClientConnection? senderConnection,
        int publisherShardId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(lease);
        long started = Stopwatch.GetTimestamp();
        cancellationToken.ThrowIfCancellationRequested();
        if (plan.Batches.Length != _shards.Count)
        {
            throw new InvalidOperationException(
                "The route plan shard count does not match the shard set.");
        }

        if (buffer.ShardCount != _shards.Count)
        {
            throw new InvalidOperationException(
                "The route plan buffer shard count does not match the shard set.");
        }

        if ((uint)publisherShardId >= (uint)_shards.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(publisherShardId));
        }

        MqttZeroFanoutOperation operation = MqttZeroFanoutOperation.Rent(
            plan,
            buffer,
            lease,
            senderConnection,
            plan.NonEmptyBatchCount,
            _deliver,
            _faulted);
        await ScheduleAndExecuteAsync(
            operation,
            plan,
            publisherShardId,
            cancellationToken).ConfigureAwait(false);
        operation.Seal();
        await operation.Completion.ConfigureAwait(false);
        int localBatchCount =
            plan.Batches[publisherShardId].Count > 0 ? 1 : 0;
        _diagnostics?.RecordPublish(
            localBatchCount,
            plan.NonEmptyBatchCount - localBatchCount,
            plan.TotalDeliveryCount,
            Stopwatch.GetTimestamp() - started);
    }

    /// <summary>
    /// 分发QosZero。
    /// </summary>
    /// <param name="plan">计划。</param>
    /// <param name="buffer">缓冲区。</param>
    /// <param name="lease">租约。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="publisherShardId">publisher分片Id。</param>
    /// <param name="returnBuffer">归还缓冲区。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask DispatchQosZeroAsync(
        MqttZeroRoutePlan plan,
        MqttZeroRoutePlanBuffer buffer,
        MqttZeroMessageLease lease,
        MqttZeroClientConnection? senderConnection,
        int publisherShardId,
        Action<MqttZeroRoutePlanBuffer> returnBuffer,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(returnBuffer);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateDispatch(plan, buffer, publisherShardId);
        MqttZeroFanoutOperation operation = MqttZeroFanoutOperation.Rent(
            plan,
            buffer,
            lease,
            senderConnection,
            plan.NonEmptyBatchCount,
            _deliver,
            _faulted,
            returnBuffer,
            autoRelease: true);
        await ScheduleAndExecuteAsync(
            operation,
            plan,
            publisherShardId,
            cancellationToken).ConfigureAwait(false);
        operation.Seal();
    }

    /// <summary>
    /// 分发Scheduled。
    /// </summary>
    /// <param name="plan">计划。</param>
    /// <param name="buffer">缓冲区。</param>
    /// <param name="lease">租约。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="publisherShardId">publisher分片Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <param name="returnBuffer">归还缓冲区。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask DispatchScheduledAsync(
        MqttZeroRoutePlan plan,
        MqttZeroRoutePlanBuffer buffer,
        MqttZeroMessageLease lease,
        MqttZeroClientConnection? senderConnection,
        int publisherShardId,
        CancellationToken cancellationToken,
        Action<MqttZeroRoutePlanBuffer>? returnBuffer = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(lease);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateDispatch(plan, buffer, publisherShardId);
        long started = Stopwatch.GetTimestamp();
        MqttZeroFanoutOperation operation = MqttZeroFanoutOperation.Rent(
            plan,
            buffer,
            lease,
            senderConnection,
            plan.NonEmptyBatchCount,
            _deliver,
            _faulted,
            returnBuffer,
            autoRelease: true);
        Exception? schedulingFailure = null;
        for (int shardId = 0; shardId < plan.Batches.Length; shardId++)
        {
            if (plan.Batches[shardId].Count == 0)
            {
                continue;
            }

            try
            {
                await _shards
                    .GetFanoutScheduler(shardId)
                    .EnqueueAsync(
                        operation,
                        shardId,
                        CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                operation.Fail(exception);
                operation.CompleteBatch();
                schedulingFailure ??= exception;
            }
        }

        operation.Seal();
        int localBatchCount =
            plan.Batches[publisherShardId].Count > 0 ? 1 : 0;
        _diagnostics?.RecordPublish(
            localBatchCount,
            plan.NonEmptyBatchCount - localBatchCount,
            plan.TotalDeliveryCount,
            Stopwatch.GetTimestamp() - started);
        if (schedulingFailure != null)
        {
            ExceptionDispatchInfo.Capture(schedulingFailure).Throw();
        }
    }

    /// <summary>
    /// 调度AndExecute。
    /// </summary>
    /// <param name="operation">对应的 operation。</param>
    /// <param name="plan">计划。</param>
    /// <param name="publisherShardId">publisher分片Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ScheduleAndExecuteAsync(
        MqttZeroFanoutOperation operation,
        MqttZeroRoutePlan plan,
        int publisherShardId,
        CancellationToken cancellationToken)
    {
        for (int shardId = 0; shardId < plan.Batches.Length; shardId++)
        {
            if (shardId == publisherShardId ||
                plan.Batches[shardId].Count == 0)
            {
                continue;
            }

            MqttZeroFanoutBatchWork work =
                MqttZeroFanoutBatchWork.Rent(operation, shardId);
            try
            {
                if (!_shards[shardId].TryEnqueue(work))
                {
                    await _shards[shardId].EnqueueAsync(
                        work,
                        CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch (Exception exception)
            {
                work.Release();
                operation.Fail(exception);
                operation.CompleteBatch();
            }
        }

        if (plan.Batches[publisherShardId].Count > 0)
        {
            try
            {
                await operation.ExecuteBatchAsync(
                    publisherShardId,
                    CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                operation.Fail(exception);
            }
            finally
            {
                operation.CompleteBatch();
            }
        }

    }

    /// <summary>
    /// 校验Dispatch。
    /// </summary>
    /// <param name="plan">计划。</param>
    /// <param name="buffer">缓冲区。</param>
    /// <param name="publisherShardId">publisher分片Id。</param>
    private void ValidateDispatch(
        MqttZeroRoutePlan plan,
        MqttZeroRoutePlanBuffer buffer,
        int publisherShardId)
    {
        if (plan.Batches.Length != _shards.Count)
        {
            throw new InvalidOperationException(
                "The route plan shard count does not match the shard set.");
        }

        if (buffer.ShardCount != _shards.Count)
        {
            throw new InvalidOperationException(
                "The route plan buffer shard count does not match the shard set.");
        }

        if ((uint)publisherShardId >= (uint)_shards.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(publisherShardId));
        }
    }
}
