namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkLoadSnapshot(
    double ProcessCpuMilliseconds,
    long WorkingSetBytes,
    long AllocatedBytes,
    int Gen0CollectionCount,
    int Gen1CollectionCount,
    int Gen2CollectionCount,
    long ThreadPoolPendingWorkItemCount,
    long ThreadPoolCompletedWorkItemCount);
