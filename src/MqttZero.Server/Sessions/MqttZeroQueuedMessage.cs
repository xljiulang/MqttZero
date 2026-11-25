using MqttZero.Memory;
using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroQueuedMessage
{
    private static readonly ConcurrentQueue<MqttZeroQueuedMessage> MessagePool = new();

    private MqttZeroMessageLease _lease = null!;
    private uint[] _subscriptionIdentifierBuffer = [];

    internal MqttZeroQueuedMessage? QueueNext { get; set; }

    internal MqttZeroQueuedMessage? QueuePrevious { get; set; }

    internal MqttZeroQueuedMessage? Qos0Next { get; set; }

    internal MqttZeroQueuedMessage? Qos0Previous { get; set; }

    internal long QueueVersion { get; set; }

    internal bool IsQueued { get; set; }

    internal bool IsQos0Queued { get; set; }

    private MqttZeroQueuedMessage()
    {
    }

    internal MqttZeroMessageLease Lease
    {
        get
        {
            return _lease;
        }
    }

    public ReadOnlyMemory<byte> Topic
    {
        get
        {
            return _lease.Topic;
        }
    }

    public ReadOnlyMemory<byte> Payload
    {
        get
        {
            return _lease.Payload;
        }
    }

    public ReadOnlyMemory<byte> Properties
    {
        get
        {
            return _lease.Properties;
        }
    }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; private set; }

    public bool Retain { get; private set; }

    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; private set; }

    public DateTimeOffset? ExpiresAt { get; private set; }

    public ushort PacketIdentifier { get; set; }

    public MqttZeroOutboundState State { get; private set; }

    public long MessageId { get; private set; }

    public int EncodedLength { get; private set; }

    private uint _encodedLengthMaximumPacketSize;

    public static MqttZeroQueuedMessage Rent(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? subscriptionIdentifier,
        DateTimeOffset? expiresAt)
    {
        return RentWithIdentifiers(
            lease,
            qualityOfServiceLevel,
            retain,
            subscriptionIdentifier.HasValue
                ? new[] { subscriptionIdentifier.Value }
                : ReadOnlyMemory<uint>.Empty,
            expiresAt);
    }

    public static MqttZeroQueuedMessage RentWithIdentifiers(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        DateTimeOffset? expiresAt)
    {
        if (!MessagePool.TryDequeue(out MqttZeroQueuedMessage? message))
        {
            message = new MqttZeroQueuedMessage();
        }

        lease.EnsureOwned();
        lease.Retain();
        message._lease = lease;
        message.QualityOfServiceLevel = qualityOfServiceLevel;
        message.Retain = retain;
        message.SubscriptionIdentifiers = subscriptionIdentifiers;
        message.ExpiresAt = expiresAt;
        message.PacketIdentifier = 0;
        message.MessageId = 0;
        message.State = qualityOfServiceLevel switch
        {
            MqttQualityOfServiceLevel.ExactlyOnce => MqttZeroOutboundState.AwaitPubRec,
            MqttQualityOfServiceLevel.AtLeastOnce => MqttZeroOutboundState.AwaitPubAck,
            _ => MqttZeroOutboundState.AwaitPubAck
        };
        return message;
    }

    public static MqttZeroQueuedMessage RentWithCopiedIdentifiers(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        DateTimeOffset? expiresAt)
    {
        MqttZeroQueuedMessage message = RentWithIdentifiers(
            lease,
            qualityOfServiceLevel,
            retain,
            ReadOnlyMemory<uint>.Empty,
            expiresAt);
        if (subscriptionIdentifiers.IsEmpty)
        {
            return message;
        }

        try
        {
            uint[] buffer = ArrayPool<uint>.Shared.Rent(
                subscriptionIdentifiers.Length);
            subscriptionIdentifiers.Span.CopyTo(buffer);
            message._subscriptionIdentifierBuffer = buffer;
            message.SubscriptionIdentifiers =
                buffer.AsMemory(0, subscriptionIdentifiers.Length);
            return message;
        }
        catch
        {
            message.Release();
            throw;
        }
    }

    public void SetAwaitPubComp()
    {
        State = MqttZeroOutboundState.AwaitPubComp;
    }

    public void SetAwaitPubRec()
    {
        State = MqttZeroOutboundState.AwaitPubRec;
    }

    public void SetMessageId(long messageId)
    {
        MessageId = messageId;
    }

    public void SetEncodedLength(int encodedLength, uint maximumPacketSize)
    {
        EncodedLength = encodedLength;
        _encodedLengthMaximumPacketSize = maximumPacketSize;
    }

    public bool IsEncodedLengthValidFor(uint maximumPacketSize)
    {
        return EncodedLength > 0 &&
            _encodedLengthMaximumPacketSize == maximumPacketSize &&
            ExpiresAt == null;
    }

    public void Restore(ushort packetIdentifier, MqttZeroOutboundState state)
    {
        PacketIdentifier = packetIdentifier;
        State = state;
    }

    public void Restore(long messageId, ushort packetIdentifier, MqttZeroOutboundState state)
    {
        MessageId = messageId;
        PacketIdentifier = packetIdentifier;
        State = state;
    }

    public bool IsExpired(DateTimeOffset now)
    {
        return ExpiresAt.HasValue && ExpiresAt.Value <= now;
    }

    public MqttPublishPacket CreatePacket(bool duplicate)
    {
        bool decoded = MqttPublishPropertiesCodec.TryDecode(
            Properties.Length == 0
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(Properties),
            out MqttPublishProperties properties,
            out IReadOnlyList<uint> subscriptionIdentifiers,
            out ushort? topicAlias);
        if (!decoded)
        {
            throw new InvalidOperationException("The queued MQTT PUBLISH properties are invalid.");
        }

        return new MqttPublishPacket
        {
            TopicName = new ReadOnlySequence<byte>(Topic),
            Payload = new ReadOnlySequence<byte>(Payload),
            Properties = properties,
            SubscriptionIdentifiers = SubscriptionIdentifiers.IsEmpty
                ? subscriptionIdentifiers
                : SubscriptionIdentifiers.Span.ToArray(),
            TopicAlias = topicAlias,
            QualityOfServiceLevel = QualityOfServiceLevel,
            PacketIdentifier = PacketIdentifier,
            Duplicate = duplicate,
            Retain = Retain
        };
    }

    public MqttZeroPublishView CreateView(bool duplicate)
    {
        return new MqttZeroPublishView(
            _lease.TopicSequence,
            _lease.PropertiesSequence,
            _lease.PayloadSequence,
            QualityOfServiceLevel,
            PacketIdentifier,
            duplicate,
            Retain,
            _lease.MessageExpiryInterval,
            null,
            SubscriptionIdentifiers,
            null);
    }

    public void Release()
    {
        ObjectDisposedException.ThrowIf(_lease is null, this);
        if (_subscriptionIdentifierBuffer.Length > 0)
        {
            ArrayPool<uint>.Shared.Return(
                _subscriptionIdentifierBuffer);
            _subscriptionIdentifierBuffer = [];
        }

        _lease.Release();
        _lease = null!;
        SubscriptionIdentifiers = ReadOnlyMemory<uint>.Empty;
        ExpiresAt = null;
        PacketIdentifier = 0;
        MessageId = 0;
        EncodedLength = 0;
        _encodedLengthMaximumPacketSize = 0;
        QueueNext = null;
        QueuePrevious = null;
        Qos0Next = null;
        Qos0Previous = null;
        IsQueued = false;
        IsQos0Queued = false;
        QueueVersion++;
        State = MqttZeroOutboundState.AwaitPubAck;
        MessagePool.Enqueue(this);
    }
}
