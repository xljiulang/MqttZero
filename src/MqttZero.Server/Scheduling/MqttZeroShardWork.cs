namespace MqttZero.Server.Scheduling;

internal abstract class MqttZeroShardWork
{
    internal long EnqueuedTimestamp { get; private set; }

    public virtual bool IsLowPriority
    {
        get
        {
            return false;
        }
    }

    public abstract ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken);

    public virtual void Release()
    {
    }

    internal void MarkEnqueued()
    {
        EnqueuedTimestamp = System.Diagnostics.Stopwatch.GetTimestamp();
    }
}
