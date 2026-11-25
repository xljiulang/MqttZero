using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutSchedulerWork : MqttZeroShardWork
{
    private readonly MqttZeroFanoutScheduler _scheduler;

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
