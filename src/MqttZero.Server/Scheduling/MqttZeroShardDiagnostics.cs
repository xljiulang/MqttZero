namespace MqttZero.Server.Scheduling;

internal sealed record MqttZeroShardDiagnostics(
    int ShardId,
    long EnqueuedCount,
    long CompletedCount,
    long FailedCount,
    int QueuedCount,
    bool IsDraining,
    double MaxDrainMilliseconds,
    double OldestWorkAgeMilliseconds);
