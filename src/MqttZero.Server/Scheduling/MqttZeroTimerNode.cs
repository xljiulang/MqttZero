namespace MqttZero.Server.Scheduling;

/// <summary>
/// 时间轮上的侵入式到期节点。
/// </summary>
internal sealed class MqttZeroTimerNode
{
    /// <summary>
    /// 初始化 MqttZeroTimerNode 的新实例。
    /// </summary>
    /// <param name="kind">种类。</param>
    /// <param name="id">对应的 id。</param>
    /// <param name="owner">持有者。</param>
    internal MqttZeroTimerNode(
        MqttZeroDeadlineKind kind,
        string id,
        object owner)
    {
        Kind = kind;
        Id = id ?? throw new ArgumentNullException(nameof(id));
        Owner = owner ?? throw new ArgumentNullException(nameof(owner));
    }

    /// <summary>
    /// 种类。
    /// </summary>
    internal MqttZeroDeadlineKind Kind { get; }

    /// <summary>
    /// Id 的值。
    /// </summary>
    internal string Id { get; }

    /// <summary>
    /// 持有者。
    /// </summary>
    internal object Owner { get; }

    /// <summary>
    /// DueUtc时钟滴答。
    /// </summary>
    internal long DueUtcTicks { get; set; }

    /// <summary>
    /// 版本。
    /// </summary>
    internal long Version { get; set; }

    /// <summary>
    /// 序列。
    /// </summary>
    internal long Sequence { get; set; }

    /// <summary>
    /// Level 的值。
    /// </summary>
    internal int Level { get; set; } = -1;

    /// <summary>
    /// Slot 的值。
    /// </summary>
    internal int Slot { get; set; } = -1;

    /// <summary>
    /// Bucket前一个。
    /// </summary>
    internal MqttZeroTimerNode? BucketPrevious { get; set; }

    /// <summary>
    /// Bucket下一个。
    /// </summary>
    internal MqttZeroTimerNode? BucketNext { get; set; }

    /// <summary>
    /// 时间轮。
    /// </summary>
    internal MqttZeroShardTimerWheel? Wheel { get; set; }

    internal bool IsScheduled
    {
        get
        {
            return Level >= 0;
        }
    }
}
