namespace MqttZero.Server.Broker;

/// <summary>
/// Broker 内存与资源快照。
/// </summary>
internal sealed record MqttZeroBrokerMemorySnapshot(
    long BufferRentedBytes,
    long BufferCachedBytes,
    long BufferCommittedBytes,
    int RoutePlanAvailableCount,
    int RoutePlanInUseCount,
    long RoutePlanRetainedBytes,
    int SharedCursorCount,
    MqttZeroObjectPoolSnapshot ObjectPools,
    int PendingDeadlineCount);
