using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Scheduling;
namespace MqttZero.Server.Retained;

/// <summary>
/// 保留消息。
/// </summary>
internal sealed class MqttZeroRetainedMessage
{
    private MqttZeroTimerNode? _expiryTimer;

    /// <summary>
    /// 初始化 MqttZeroRetainedMessage 的新实例。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    public MqttZeroRetainedMessage(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        DateTimeOffset? expiresAt)
    {
        lease.Retain();
        Lease = lease;
        QualityOfServiceLevel = qualityOfServiceLevel;
        ExpiresAt = expiresAt;
    }

    /// <summary>
    /// 租约。
    /// </summary>
    public MqttZeroMessageLease Lease { get; }

    public ReadOnlyMemory<byte> Topic
    {
        get
        {
            return Lease.Topic;
        }
    }

    public ReadOnlyMemory<byte> Payload
    {
        get
        {
            return Lease.Payload;
        }
    }

    /// <summary>
    /// 服务质量ServiceLevel。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    /// <summary>
    /// ExpiresAt 的值。
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; }

    public long ByteLength
    {
        get
        {
            return Topic.Length + Lease.Properties.Length + Payload.Length;
        }
    }

    /// <summary>
    /// 保留。
    /// </summary>
    public void Retain()
    {
        Lease.Retain();
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Release()
    {
        Lease.Release();
    }

    /// <summary>
    /// 获取Expiry定时器节点。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <returns>对应的结果。</returns>
    internal MqttZeroTimerNode GetExpiryTimerNode(string topic)
    {
        MqttZeroTimerNode? current = Volatile.Read(
            ref _expiryTimer);
        if (current != null)
        {
            return current;
        }

        MqttZeroTimerNode candidate = new(
            MqttZeroDeadlineKind.RetainedExpiry,
            topic,
            this);
        return Interlocked.CompareExchange(
            ref _expiryTimer,
            candidate,
            null) ?? candidate;
    }
}
