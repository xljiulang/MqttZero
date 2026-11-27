namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Broker-owned memory and process counters reported by a probe line.
/// </summary>
internal sealed record BrokerMemoryProbeSnapshot(
    long TimestampTicks,
    long BufferRentedBytes,
    long BufferCachedBytes,
    long BufferCommittedBytes,
    int RoutePlanAvailableCount,
    int RoutePlanInUseCount,
    long RoutePlanRetainedBytes,
    int SharedCursorCount,
    int PendingDeadlineCount,
    int LeaseCount,
    int QueuedMessageCount,
    int PublishContextCount,
    int InboundWorkCount,
    int InboundShardWorkCount,
    int FanoutOperationCount,
    int FanoutBatchWorkCount,
    int FanoutScheduledBatchCount,
    int AcknowledgementWorkCount,
    int AcknowledgementShardWorkCount,
    long ManagedHeapBytes,
    long WorkingSetBytes,
    long AllocatedBytes,
    int Gen2CollectionCount);
