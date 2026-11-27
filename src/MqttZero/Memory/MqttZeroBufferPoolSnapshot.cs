namespace MqttZero.Memory;

/// <summary>
/// 缓冲区池的租出、缓存与提交字节快照。
/// </summary>
internal sealed record MqttZeroBufferPoolSnapshot(
    long RentedBytes,
    long CachedBytes,
    long CommittedBytes,
    MqttZeroBufferBucketSnapshot[] Buckets);
