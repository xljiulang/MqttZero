namespace MqttZero.Server.Scheduling;

/// <summary>
/// 分片队列与执行的统计快照。
/// </summary>
internal sealed record MqttZeroShardDiagnostics(
    int ShardId,
    long EnqueuedCount,
    long CompletedCount,
    long FailedCount,
    int QueuedCount,
    bool IsDraining,
    double MaxDrainMilliseconds,
    double OldestWorkAgeMilliseconds,
    int HighPriorityQueuedCount,
    int LowPriorityQueuedCount,
    double HighPriorityOldestWorkAgeMilliseconds,
    double LowPriorityOldestWorkAgeMilliseconds,
    long LowPriorityWaitCount);
