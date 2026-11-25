using MqttZero.Memory;
using MqttZero.Protocol;
namespace MqttZero.Server.Retained;

internal sealed class MqttZeroRetainedMessage
{
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

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    public DateTimeOffset? ExpiresAt { get; }

    public long ByteLength
    {
        get
        {
            return Topic.Length + Lease.Properties.Length + Payload.Length;
        }
    }

    public void Retain()
    {
        Lease.Retain();
    }

    public void Release()
    {
        Lease.Release();
    }
}
