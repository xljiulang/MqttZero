namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkTopologyInfo(
    int EventLoopShardCount,
    int ResolvedEventLoopShardCount,
    int IoQueueCount,
    int ResolvedIoQueueCount);
