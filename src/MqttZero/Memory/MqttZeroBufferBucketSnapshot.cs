namespace MqttZero.Memory;

/// <summary>
/// 单个缓冲区桶的容量与命中统计。
/// </summary>
internal sealed record MqttZeroBufferBucketSnapshot(
    int BufferSize,
    int CachedCount,
    long CachedBytes);
