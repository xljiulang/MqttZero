namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkFanoutDiagnostics(
    long PublishCount,
    long LocalBatchCount,
    long RemoteBatchCount,
    long DeliveryCount,
    double AverageFanoutMicroseconds,
    double MaxFanoutMicroseconds,
    long EgressSignalCount,
    long EgressDrainCount,
    long EncodedPublishCount,
    long FlushWaitCount,
    int MaxPendingQueueDepth);
