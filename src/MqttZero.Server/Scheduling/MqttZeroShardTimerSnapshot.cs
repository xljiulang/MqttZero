namespace MqttZero.Server.Scheduling;

/// <summary>
/// 表示 struct 的类型。
/// </summary>
internal readonly record struct MqttZeroShardTimerSnapshot(
    int ShardId,
    int Count,
    long CurrentBaseTick,
    IReadOnlyList<int> LevelCounts,
    double OldestTimerAgeMilliseconds);
