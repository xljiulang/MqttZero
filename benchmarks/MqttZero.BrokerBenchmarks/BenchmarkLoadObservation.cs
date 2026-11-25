namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkLoadObservation(
    double ProcessCpuMilliseconds,
    long WorkingSetBytes,
    long AllocatedBytesDelta,
    int Gen0CollectionCountDelta,
    int Gen1CollectionCountDelta,
    int Gen2CollectionCountDelta,
    long ThreadPoolPendingWorkItemCount,
    long ThreadPoolCompletedWorkItemCountDelta);
