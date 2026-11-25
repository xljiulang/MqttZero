using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressSchedulerWork : MqttZeroShardWork
{
    private readonly MqttZeroEgressScheduler _scheduler;

    public MqttZeroEgressSchedulerWork(
        MqttZeroEgressScheduler scheduler)
    {
        _scheduler = scheduler;
    }

    public override bool IsLowPriority
    {
        get
        {
            return true;
        }
    }

    public override ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        return _scheduler.ExecuteAsync(cancellationToken);
    }

    public override void Release()
    {
        _scheduler.OnWorkReleased();
    }
}
