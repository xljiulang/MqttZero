using MqttZero.Memory;
using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Egress;

/// <summary>
/// 把控制帧移交出站分片而不等待写出。
/// </summary>
internal sealed class MqttZeroControlPostWork : MqttZeroShardWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroControlPostWork> Pool = new(MqttZeroBoundedObjectPool<MqttZeroControlPostWork>.DefaultCapacity);
    private MqttZeroEgressControlFrame? _frame;
    private MqttZeroSessionEgress? _egress;

    /// <summary>
    /// 初始化 MqttZeroControlPostWork 的新实例。
    /// </summary>
    private MqttZeroControlPostWork()
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
    /// <param name="egress">出站。</param>
    /// <param name="frame">帧。</param>
    /// <returns>对应的结果。</returns>
    internal static MqttZeroControlPostWork Rent(
        MqttZeroSessionEgress egress,
        MqttZeroEgressControlFrame frame)
    {
        ArgumentNullException.ThrowIfNull(egress);
        ArgumentNullException.ThrowIfNull(frame);
        if (!Pool.TryRent(out MqttZeroControlPostWork? work) ||
            work is null)
        {
            work = new MqttZeroControlPostWork();
        }

        work._egress = egress;
        work._frame = frame;
        return work;
    }

    /// <summary>
    /// 执行。
    /// </summary>
    /// <param name="shard">分片。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public override ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        MqttZeroSessionEgress egress = _egress ??
            throw new InvalidOperationException(
                "The control post work was not initialized.");
        MqttZeroEgressControlFrame frame = _frame ??
            throw new InvalidOperationException(
                "The control post frame was not initialized.");
        _frame = null;
        egress.AcceptControlFromShard(frame);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public override void Release()
    {
        MqttZeroEgressControlFrame? frame = _frame;
        _frame = null;
        _egress = null;
        if (frame != null)
        {
            frame.Fail(new InvalidOperationException(
                "The control frame was never handed to the egress."));
            frame.Release();
        }

        _ = Pool.Return(this);
    }
}
