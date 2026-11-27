namespace MqttZero.Server.Routing;

/// <summary>
/// 路由计划缓冲区池快照。
/// </summary>
internal sealed record MqttZeroRoutePlanBufferPoolSnapshot(
    int AvailableCount,
    int InUseCount,
    long RetainedBytes);
