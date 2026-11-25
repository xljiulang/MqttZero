namespace MqttZero.Server.Broker;

internal sealed record MqttZeroFanoutDiagnosticsSnapshot(
    long PublishCount,
    long LocalBatchCount,
    long RemoteBatchCount,
    long DeliveryCount,
    long TotalFanoutTicks,
    long MaxFanoutTicks,
    long EgressSignalCount,
    long EgressDrainCount,
    long EncodedPublishCount,
    long FlushWaitCount,
    int MaxPendingQueueDepth);
