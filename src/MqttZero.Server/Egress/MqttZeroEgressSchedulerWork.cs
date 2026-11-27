using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Egress;

/// <summary>
/// 出站调度的分片工作项。
/// </summary>
internal sealed class MqttZeroEgressSchedulerWork : MqttZeroShardWork
{
    private readonly MqttZeroEgressScheduler _scheduler;

    /// <summary>
    /// 初始化 MqttZeroEgressSchedulerWork 的新实例。
    /// </summary>
    /// <param name="scheduler">调度器。</param>
    public MqttZeroEgressSchedulerWork(
        MqttZeroEgressScheduler scheduler)
    {
        _scheduler = scheduler;
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
