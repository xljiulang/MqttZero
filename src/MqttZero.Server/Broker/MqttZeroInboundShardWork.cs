using MqttZero.Memory;
using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

/// <summary>
/// 把入站发布调度到会话分片的包装。
/// </summary>
internal sealed class MqttZeroInboundShardWork : MqttZeroShardWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroInboundShardWork> Pool = new(MqttZeroBoundedObjectPool<MqttZeroInboundShardWork>.DefaultCapacity);
    private Action<Exception>? _faulted;
    private Func<MqttZeroInboundWork, CancellationToken, ValueTask>? _route;
    private MqttZeroInboundWork? _work;

    /// <summary>
    /// 初始化 MqttZeroInboundShardWork 的新实例。
    /// </summary>
    private MqttZeroInboundShardWork()
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
    /// <param name="route">对应的 route。</param>
    /// <param name="faulted">故障回调。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroInboundShardWork Rent(
        MqttZeroInboundWork work,
        Func<MqttZeroInboundWork, CancellationToken, ValueTask> route,
        Action<Exception> faulted)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(route);
        ArgumentNullException.ThrowIfNull(faulted);
        if (!Pool.TryRent(out MqttZeroInboundShardWork? shardWork) ||
            shardWork is null)
        {
            shardWork = new MqttZeroInboundShardWork();
        }

        shardWork._work = work;
        shardWork._route = route;
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
        MqttZeroInboundWork work = _work ??
            throw new InvalidOperationException(
                "The inbound shard work was not initialized.");
        Func<MqttZeroInboundWork, CancellationToken, ValueTask> route =
            _route ??
                throw new InvalidOperationException(
                    "The inbound shard route was not initialized.");
        try
        {
            await route(work, cancellationToken).ConfigureAwait(false);
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
        _work?.Release();
        _work = null;
        _route = null;
        _faulted = null;
        _ = Pool.Return(this);
    }
}
