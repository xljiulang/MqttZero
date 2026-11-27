namespace MqttZero.Server.Scheduling;

/// <summary>
/// 驱动时间轮一次 tick 的工作项。
/// </summary>
internal sealed class MqttZeroTimerTickWork : MqttZeroShardWork
{
    private readonly MqttZeroShardTimerWheel _wheel;

    /// <summary>
    /// 初始化 MqttZeroTimerTickWork 的新实例。
    /// </summary>
    /// <param name="wheel">时间轮。</param>
    internal MqttZeroTimerTickWork(MqttZeroShardTimerWheel wheel)
    {
        _wheel = wheel;
    }

    /// <summary>
    /// IsLowPriority 的值。
    /// </summary>
    public override bool IsLowPriority
    {
        get
        {
            return true;
        }
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
        return _wheel.ExecuteTickWorkAsync(cancellationToken);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public override void Release()
    {
        _wheel.OnTickWorkReleased();
    }
}
