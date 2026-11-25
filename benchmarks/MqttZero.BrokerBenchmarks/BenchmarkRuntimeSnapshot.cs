namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkRuntimeSnapshot(
    bool IsServerGarbageCollection,
    bool IsConcurrentGarbageCollection,
    bool IsTieredPgoEnabled,
    int MinimumWorkerThreads,
    int MaximumWorkerThreads,
    int MinimumCompletionPortThreads,
    int MaximumCompletionPortThreads);
