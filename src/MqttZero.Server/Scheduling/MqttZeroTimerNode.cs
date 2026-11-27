namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroTimerNode
{
    internal MqttZeroTimerNode(
        MqttZeroDeadlineKind kind,
        string id,
        object owner)
    {
        Kind = kind;
        Id = id ?? throw new ArgumentNullException(nameof(id));
        Owner = owner ?? throw new ArgumentNullException(nameof(owner));
    }

    internal MqttZeroDeadlineKind Kind { get; }

    internal string Id { get; }

    internal object Owner { get; }

    internal long DueUtcTicks { get; set; }

    internal long Version { get; set; }

    internal long Sequence { get; set; }

    internal int Level { get; set; } = -1;

    internal int Slot { get; set; } = -1;

    internal MqttZeroTimerNode? BucketPrevious { get; set; }

    internal MqttZeroTimerNode? BucketNext { get; set; }

    internal MqttZeroShardTimerWheel? Wheel { get; set; }

    internal bool IsScheduled
    {
        get
        {
            return Level >= 0;
        }
    }
}
