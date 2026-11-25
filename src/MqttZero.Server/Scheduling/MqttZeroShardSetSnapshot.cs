namespace MqttZero.Server.Scheduling;

internal sealed record MqttZeroShardSetSnapshot(
    IReadOnlyList<MqttZeroShardDiagnostics> Shards);
