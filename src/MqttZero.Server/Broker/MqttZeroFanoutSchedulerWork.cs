using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

/// <summary>
/// 扇出调度的分片工作项。
/// </summary>
internal sealed class MqttZeroFanoutSchedulerWork : MqttZeroShardWork
{
    private readonly MqttZeroFanoutScheduler _scheduler;

    /// <summary>
    /// 初始化 MqttZeroFanoutSchedulerWork 的新实例。
    /// </summary>
    /// <param name="scheduler">调度器。</param>
    internal MqttZeroFanoutSchedulerWork(
        MqttZeroFanoutScheduler scheduler)
    {
        _scheduler = scheduler;
    }

    public override bool IsLowPriority
    {
        get
        {
            return false;
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
        return _scheduler.ExecuteAsync(cancellationToken);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public override void Release()
    {
        _scheduler.OnWorkReleased();
    }
}
