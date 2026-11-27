namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroTimerTickWork : MqttZeroShardWork
{
    private readonly MqttZeroShardTimerWheel _wheel;

    internal MqttZeroTimerTickWork(MqttZeroShardTimerWheel wheel)
    {
        _wheel = wheel;
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
        return _wheel.ExecuteTickWorkAsync(cancellationToken);
    }

    public override void Release()
    {
        _wheel.OnTickWorkReleased();
    }
}
