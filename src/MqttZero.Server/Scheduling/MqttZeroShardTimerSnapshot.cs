namespace MqttZero.Server.Scheduling;

internal readonly record struct MqttZeroShardTimerSnapshot(
    int ShardId,
    int Count,
    long CurrentBaseTick,
    IReadOnlyList<int> LevelCounts,
    double OldestTimerAgeMilliseconds);
