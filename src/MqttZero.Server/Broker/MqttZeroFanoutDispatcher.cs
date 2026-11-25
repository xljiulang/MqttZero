using MqttZero.Memory;
using MqttZero.Server.Clients;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using System.Diagnostics;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutDispatcher
{
    private readonly Func<
        MqttZeroPlannedDelivery,
        MqttZeroMessageLease,
        MqttZeroClientConnection?,
        CancellationToken,
        ValueTask> _deliver;
    private readonly MqttZeroFanoutDiagnostics? _diagnostics;
    private readonly Action<Exception> _faulted;
    private readonly MqttZeroShardSet _shards;

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
                await _shards[shardId].EnqueueAsync(
                    work,
                    CancellationToken.None).ConfigureAwait(false);
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
}
