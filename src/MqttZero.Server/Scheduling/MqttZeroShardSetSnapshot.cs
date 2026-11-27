namespace MqttZero.Server.Scheduling;

/// <summary>
/// 分片集合的诊断快照。
/// </summary>
internal sealed record MqttZeroShardSetSnapshot(
    IReadOnlyList<MqttZeroShardDiagnostics> Shards);
