using MqttZero.Memory;
using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

/// <summary>
/// 把确认工作项调度到会话分片的包装。
/// </summary>
internal sealed class MqttZeroAcknowledgementShardWork : MqttZeroShardWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroAcknowledgementShardWork> Pool = new(MqttZeroBoundedObjectPool<MqttZeroAcknowledgementShardWork>.DefaultCapacity);
    private MqttZeroAcknowledgementWork? _acknowledgementWork;
    private Action<Exception>? _faulted;
    private Func<MqttZeroAcknowledgementWork, CancellationToken, ValueTask>? _process;

    /// <summary>
    /// 初始化 MqttZeroAcknowledgementShardWork 的新实例。
    /// </summary>
    private MqttZeroAcknowledgementShardWork()
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
    /// <param name="work">工作项。</param>
    /// <param name="process">对应的 process。</param>
    /// <param name="faulted">故障回调。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroAcknowledgementShardWork Rent(
        MqttZeroAcknowledgementWork work,
        Func<
            MqttZeroAcknowledgementWork,
            CancellationToken,
            ValueTask> process,
        Action<Exception> faulted)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(process);
        ArgumentNullException.ThrowIfNull(faulted);
        if (!Pool.TryRent(
                out MqttZeroAcknowledgementShardWork? shardWork) ||
            shardWork is null)
        {
            shardWork = new MqttZeroAcknowledgementShardWork();
        }

        shardWork._acknowledgementWork = work;
        shardWork._process = process;
        shardWork._faulted = faulted;
        return shardWork;
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
        MqttZeroAcknowledgementWork work =
            _acknowledgementWork ??
                throw new InvalidOperationException(
                    "The acknowledgement work was not initialized.");
        Func<
            MqttZeroAcknowledgementWork,
            CancellationToken,
            ValueTask> process =
            _process ??
                throw new InvalidOperationException(
                    "The acknowledgement processor was not initialized.");
        try
        {
            await process(work, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _faulted?.Invoke(exception);
            throw;
        }
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public override void Release()
    {
        _acknowledgementWork?.Release();
        _acknowledgementWork = null;
        _process = null;
        _faulted = null;
        _ = Pool.Return(this);
    }
}
