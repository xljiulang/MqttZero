using MqttZero.Memory;
using MqttZero.Server.Clients;
using MqttZero.Server.Routing;
using System.Threading.Tasks.Sources;

namespace MqttZero.Server.Broker;

/// <summary>
/// 一次发布对应的扇出操作与完成信号。
/// </summary>
internal sealed class MqttZeroFanoutOperation : IValueTaskSource<bool>
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroFanoutOperation> Pool = new(MqttZeroBoundedObjectPool<MqttZeroFanoutOperation>.DefaultCapacity);

    private ManualResetValueTaskSourceCore<bool> _completion = new()
    {
        RunContinuationsAsynchronously = true
    };
    private bool _autoRelease;
    private int _autoReleaseArmed;
    private MqttZeroRoutePlanBuffer? _buffer;
    private int _completed;
    private Func<MqttZeroPlannedDelivery, MqttZeroMessageLease, MqttZeroClientConnection?, CancellationToken, ValueTask>? _deliver;
    private Exception? _failure;
    private Action<Exception>? _faulted;
    private MqttZeroMessageLease? _lease;
    private MqttZeroRoutePlan _plan;
    private int _remainingBatchCount;
    private int _returned;
    private MqttZeroClientConnection? _senderConnection;
    private Action<MqttZeroRoutePlanBuffer>? _returnBuffer;
    private short _version;

    /// <summary>
    /// 初始化 MqttZeroFanoutOperation 的新实例。
    /// </summary>
    private MqttZeroFanoutOperation()
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
    /// Completion 的值。
    /// </summary>
    internal ValueTask<bool> Completion
    {
        get
        {
            return new ValueTask<bool>(this, _version);
        }
    }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="plan">计划。</param>
    /// <param name="buffer">缓冲区。</param>
    /// <param name="lease">租约。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="expectedBatchCount">expected批次数量。</param>
    /// <param name="deliver">对应的 deliver。</param>
    /// <param name="faulted">故障回调。</param>
    /// <param name="returnBuffer">归还缓冲区。</param>
    /// <param name="autoRelease">对应的 autoRelease。</param>
    /// <returns>对应的结果。</returns>
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
        Action<Exception> faulted,
        Action<MqttZeroRoutePlanBuffer>? returnBuffer = null,
        bool autoRelease = false)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentNullException.ThrowIfNull(lease);
        ArgumentNullException.ThrowIfNull(deliver);
        ArgumentNullException.ThrowIfNull(faulted);
        ArgumentOutOfRangeException.ThrowIfNegative(expectedBatchCount);
        if (!Pool.TryRent(out MqttZeroFanoutOperation? operation) ||
            operation is null)
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
        operation._returnBuffer = returnBuffer;
        operation._autoRelease = autoRelease;
        operation._autoReleaseArmed = 0;
        operation._failure = null;
        operation._remainingBatchCount = expectedBatchCount;
        operation._completed = 0;
        operation._returned = 0;
        lease.Retain();
        return operation;
    }

    /// <summary>
    /// 完成批次。
    /// </summary>
    internal void CompleteBatch()
    {
        if (Interlocked.Decrement(ref _remainingBatchCount) == 0)
        {
            Complete();
        }
    }

    /// <summary>
    /// 执行批次。
    /// </summary>
    /// <param name="shardId">分片Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 标记失败。
    /// </summary>
    /// <param name="exception">异常。</param>
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

    /// <summary>
    /// 执行 Seal 对应的处理。
    /// </summary>
    internal void Seal()
    {
        if (_autoRelease)
        {
            Volatile.Write(ref _autoReleaseArmed, 1);
        }

        Thread.MemoryBarrier();
        if (Volatile.Read(ref _remainingBatchCount) == 0)
        {
            if (_autoRelease &&
                Volatile.Read(ref _completed) != 0)
            {
                ReturnToPool();
                return;
            }

            Complete();
        }
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(
        short token)
    {
        return _completion.GetStatus(token);
    }

    /// <inheritdoc/>
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

    /// <summary>
    /// 完成。
    /// </summary>
    private void Complete()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
        {
            return;
        }

        Exception? failure = Volatile.Read(ref _failure);
        if (_autoRelease)
        {
            if (Volatile.Read(ref _autoReleaseArmed) != 0)
            {
                ReturnToPool();
            }

            return;
        }

        if (failure == null)
        {
            _completion.SetResult(true);
        }
        else
        {
            _completion.SetException(failure);
        }
    }

    /// <summary>
    /// 归还To池。
    /// </summary>
    private void ReturnToPool()
    {
        if (Interlocked.Exchange(ref _returned, 1) != 0)
        {
            return;
        }

        MqttZeroRoutePlanBuffer? buffer = _buffer;
        MqttZeroMessageLease? lease = _lease;
        Action<MqttZeroRoutePlanBuffer>? returnBuffer = _returnBuffer;
        _buffer = null;
        _deliver = null;
        _failure = null;
        _faulted = null;
        _lease = null;
        _plan = default;
        _remainingBatchCount = 0;
        _senderConnection = null;
        _returnBuffer = null;
        _autoRelease = false;
        _autoReleaseArmed = 0;
        lease?.Release();
        if (buffer != null)
        {
            returnBuffer?.Invoke(buffer);
        }

        _ = Pool.Return(this);
    }
}
