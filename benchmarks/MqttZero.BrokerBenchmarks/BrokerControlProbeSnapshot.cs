namespace MqttZero.BrokerBenchmarks;

internal sealed record BrokerControlProbeSnapshot(
    long TimestampTicks,
    long PublishCount,
    long DeliveryCount,
    long ControlFrameCount,
    long ControlFrameDirectCount,
    long ControlFrameQueuedCount,
    long ControlBatchCount,
    long ControlBatchFrameSum,
    long ControlBatchFrameMax,
    long TotalControlWriteTicks,
    long MaxControlWriteTicks,
    long AckWorkCount,
    long TotalAckWorkQueueTicks,
    long MaxAckWorkQueueTicks,
    long QosZeroDirectCount,
    long QosZeroDeferredCount,
    long EgressDataBatchCount,
    long EgressBatchMessageCount,
    long EgressFlushStartCount,
    long FlushWaitCount,
    int MaxPendingQueueDepth);
