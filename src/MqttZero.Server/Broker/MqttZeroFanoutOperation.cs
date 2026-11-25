using MqttZero.Memory;
using MqttZero.Server.Clients;
using MqttZero.Server.Routing;
using System.Collections.Concurrent;
using System.Threading.Tasks.Sources;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutOperation : IValueTaskSource<bool>
{
    private static readonly ConcurrentQueue<MqttZeroFanoutOperation> Pool =
        new();

    private ManualResetValueTaskSourceCore<bool> _completion = new()
    {
        RunContinuationsAsynchronously = true
    };
    private MqttZeroRoutePlanBuffer? _buffer;
    private int _completed;
    private Func<
        MqttZeroPlannedDelivery,
        MqttZeroMessageLease,
        MqttZeroClientConnection?,
        CancellationToken,
        ValueTask>? _deliver;
    private Exception? _failure;
    private Action<Exception>? _faulted;
    private MqttZeroMessageLease? _lease;
    private MqttZeroRoutePlan _plan;
    private int _remainingBatchCount;
    private MqttZeroClientConnection? _senderConnection;
    private short _version;

    private MqttZeroFanoutOperation()
    {
    }

    internal ValueTask<bool> Completion
    {
        get
        {
            return new ValueTask<bool>(this, _version);
        }
    }

    internal static MqttZeroFanoutOperation Rent(
        MqttZeroRoutePlan plan,
        MqttZeroRoutePlanBuffer buffer,
        MqttZeroMessageLease lease,
        MqttZeroClientConnection? senderConnection,
        int expectedBatchCount,
        Func<
            MqttZeroPlannedDelivery,
            MqttZeroMessageLease,
            MqttZeroClientConnection?,
            CancellationToken,
            ValueTask> deliver,
        Action<Exception> faulted)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(deliver);
        ArgumentNullException.ThrowIfNull(faulted);
        ArgumentOutOfRangeException.ThrowIfNegative(expectedBatchCount);
        if (!Pool.TryDequeue(out MqttZeroFanoutOperation? operation))
        {
            operation = new MqttZeroFanoutOperation();
        }

        operation._completion.Reset();
        operation._version = operation._completion.Version;
        operation._plan = plan;
        operation._buffer = buffer;
        operation._lease = lease;
        operation._senderConnection = senderConnection;
        operation._deliver = deliver;
        operation._faulted = faulted;
        operation._failure = null;
        operation._remainingBatchCount = expectedBatchCount;
        operation._completed = 0;
        lease.Retain();
        return operation;
    }

    internal void CompleteBatch()
    {
        if (Interlocked.Decrement(ref _remainingBatchCount) == 0)
        {
            Complete();
        }
    }

    internal async ValueTask ExecuteBatchAsync(
        int shardId,
        CancellationToken cancellationToken)
    {
        MqttZeroRoutePlanBuffer buffer = _buffer ??
            throw new InvalidOperationException(
                "The fanout operation buffer is unavailable.");
        MqttZeroMessageLease lease = _lease ??
            throw new InvalidOperationException(
                "The fanout operation lease is unavailable.");
        Func<
            MqttZeroPlannedDelivery,
            MqttZeroMessageLease,
            MqttZeroClientConnection?,
            CancellationToken,
            ValueTask> deliver = _deliver ??
                throw new InvalidOperationException(
                    "The fanout operation delivery handler is unavailable.");
        if (_plan.Batches.Length != buffer.ShardCount)
        {
            throw new InvalidOperationException(
                "The fanout operation buffer shard count is inconsistent.");
        }

        MqttZeroFanoutBatch batch = _plan.Batches[shardId];
        for (int index = 0; index < batch.Count; index++)
        {
            await deliver(
                batch[index],
                lease,
                _senderConnection,
                cancellationToken).ConfigureAwait(false);
        }
    }

    internal void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        Exception? current = Volatile.Read(ref _failure);
        while (current == null)
        {
            Exception? observed = Interlocked.CompareExchange(
                ref _failure,
                exception,
                null);
            if (observed == null)
            {
                try
                {
                    _faulted?.Invoke(exception);
                }
                catch (Exception)
                {
                }

                return;
            }

            current = observed;
        }
    }

    internal void Seal()
    {
        Thread.MemoryBarrier();
        if (Volatile.Read(ref _remainingBatchCount) == 0)
        {
            Complete();
        }
    }

    bool IValueTaskSource<bool>.GetResult(short token)
    {
        try
        {
            return _completion.GetResult(token);
        }
        finally
        {
            ReturnToPool();
        }
    }

    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(
        short token)
    {
        return _completion.GetStatus(token);
    }

    void IValueTaskSource<bool>.OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
        _completion.OnCompleted(
            continuation,
            state,
            token,
            flags);
    }

    private void Complete()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
        {
            return;
        }

        Exception? failure = Volatile.Read(ref _failure);
        if (failure == null)
        {
            _completion.SetResult(true);
        }
        else
        {
            _completion.SetException(failure);
        }
    }

    private void ReturnToPool()
    {
        MqttZeroMessageLease? lease = _lease;
        _buffer = null;
        _deliver = null;
        _failure = null;
        _faulted = null;
        _lease = null;
        _plan = default;
        _remainingBatchCount = 0;
        _senderConnection = null;
        lease?.Release();
        Pool.Enqueue(this);
    }
}
