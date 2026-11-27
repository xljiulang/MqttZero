using System.Diagnostics;

namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkMemorySnapshot(
    long AllocatedBytes,
    long ManagedHeapBytes,
    long WorkingSetBytes,
    long PrivateMemoryBytes,
    int Gen0CollectionCount,
    int Gen1CollectionCount,
    int Gen2CollectionCount,
    long BufferRentedBytes,
    long BufferCachedBytes,
    long BufferCommittedBytes,
    int RoutePlanAvailableCount,
    int RoutePlanInUseCount,
    long RoutePlanRetainedBytes,
    int SharedCursorCount,
    MqttZero.Server.Broker.MqttZeroObjectPoolSnapshot ObjectPools)
{
    public static BenchmarkMemorySnapshot Capture(
        MqttZero.Server.Broker.MqttZeroBrokerMemorySnapshot broker)
    {
        ArgumentNullException.ThrowIfNull(broker);
        using Process process = Process.GetCurrentProcess();
        return new BenchmarkMemorySnapshot(
            GC.GetTotalAllocatedBytes(precise: true),
            GC.GetGCMemoryInfo().HeapSizeBytes,
            process.WorkingSet64,
            process.PrivateMemorySize64,
            GC.CollectionCount(0),
            GC.CollectionCount(1),
            GC.CollectionCount(2),
            broker.BufferRentedBytes,
            broker.BufferCachedBytes,
            broker.BufferCommittedBytes,
            broker.RoutePlanAvailableCount,
            broker.RoutePlanInUseCount,
            broker.RoutePlanRetainedBytes,
            broker.SharedCursorCount,
            broker.ObjectPools);
    }

    public static BenchmarkMemorySnapshot FromProbe(
        BrokerMemoryProbeSnapshot probe)
    {
        ArgumentNullException.ThrowIfNull(probe);
        return new BenchmarkMemorySnapshot(
            probe.AllocatedBytes,
            probe.ManagedHeapBytes,
            probe.WorkingSetBytes,
            probe.WorkingSetBytes,
            probe.Gen2CollectionCount,
            probe.Gen2CollectionCount,
            probe.Gen2CollectionCount,
            probe.BufferRentedBytes,
            probe.BufferCachedBytes,
            probe.BufferCommittedBytes,
            probe.RoutePlanAvailableCount,
            probe.RoutePlanInUseCount,
            probe.RoutePlanRetainedBytes,
            probe.SharedCursorCount,
            new MqttZero.Server.Broker.MqttZeroObjectPoolSnapshot(
                probe.LeaseCount,
                probe.QueuedMessageCount,
                probe.PublishContextCount,
                probe.InboundWorkCount,
                probe.InboundShardWorkCount,
                probe.FanoutOperationCount,
                probe.FanoutBatchWorkCount,
                probe.FanoutScheduledBatchCount,
                probe.AcknowledgementWorkCount,
                probe.AcknowledgementShardWorkCount));
    }
}
