using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressWork : MqttZeroShardWork
{
    private readonly MqttZeroEgressActor _actor;

    public MqttZeroEgressWork(MqttZeroEgressActor actor)
    {
        _actor = actor;
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
        return _actor.ExecuteAsync(cancellationToken);
    }

    public override void Release()
    {
        _actor.OnWorkReleased();
    }
}
