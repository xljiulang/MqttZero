using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Memory;

internal sealed class MqttZeroMessageLease : IDisposable
{
    private static readonly ConcurrentQueue<MqttZeroMessageLease> LeasePool =
        new();

    private MqttZeroBufferPool _bufferPool = MqttZeroBufferPool.Shared;
    private bool _isBorrowed;
    private int _payloadLength;
    private int _payloadOffset;
    private int _propertiesLength;
    private int _propertiesOffset;
    private readonly object _promotionGate = new();
    private int _referenceCount;
    private byte[] _slabBuffer = [];
    private int _topicLength;
    private int _topicOffset;

    private MqttZeroMessageLease()
    {
    }

    public ReadOnlyMemory<byte> Topic { get; private set; }

    public ReadOnlyMemory<byte> Properties { get; private set; }

    public ReadOnlyMemory<byte> Payload { get; private set; }

    public ReadOnlySequence<byte> TopicSequence { get; private set; }

    public ReadOnlySequence<byte> PropertiesSequence { get; private set; }

    public ReadOnlySequence<byte> PayloadSequence { get; private set; }

    public bool IsBorrowed
    {
        get
        {
            return _isBorrowed;
        }
    }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; private set; }

    public bool RetainFlag { get; private set; }

    public uint? MessageExpiryInterval { get; private set; }

    public uint? SubscriptionIdentifier { get; private set; }

    public DateTimeOffset? ExpiresAt { get; private set; }

    public ushort PacketIdentifier { get; set; }

    public static MqttZeroMessageLease CopyFrom(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        MqttZeroBufferPool? bufferPool = null)
    {
        MqttZeroMessageLease lease = RentLease();
        lease.InitializeOwned(
            topic,
            properties,
            payload,
            qualityOfServiceLevel,
            retain,
            messageExpiryInterval,
            subscriptionIdentifier,
            bufferPool ?? MqttZeroBufferPool.Shared);
        return lease;
    }

    public static MqttZeroMessageLease BorrowFrom(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        MqttZeroBufferPool? bufferPool = null)
    {
        MqttZeroMessageLease lease = RentLease();
        lease.InitializeBorrowed(
            topic,
            properties,
            payload,
            qualityOfServiceLevel,
            retain,
            messageExpiryInterval,
            subscriptionIdentifier,
            bufferPool ?? MqttZeroBufferPool.Shared);
        return lease;
    }

    public MqttZeroMessageLease CreateOwnedCopy()
    {
        MqttZeroMessageLease copy = CopyFrom(
            TopicSequence,
            PropertiesSequence,
            PayloadSequence,
            QualityOfServiceLevel,
            RetainFlag,
            MessageExpiryInterval,
            SubscriptionIdentifier,
            _bufferPool);
        copy.ExpiresAt = ExpiresAt;
        return copy;
    }

    public void SetExpiresAt(DateTimeOffset? expiresAt)
    {
        ExpiresAt = expiresAt;
    }

    public void EnsureOwned()
    {
        if (!_isBorrowed)
        {
            return;
        }

        lock (_promotionGate)
        {
            if (!_isBorrowed)
            {
                return;
            }

            int topicLength = checked((int)TopicSequence.Length);
            int propertiesLength = checked((int)PropertiesSequence.Length);
            int payloadLength = checked((int)PayloadSequence.Length);
            int totalLength = checked(
                topicLength + propertiesLength + payloadLength);
            byte[] replacement = RentSlab(totalLength);
            try
            {
                CopyRegion(
                    TopicSequence,
                    replacement.AsSpan(0, topicLength));
                CopyRegion(
                    PropertiesSequence,
                    replacement.AsSpan(
                        topicLength,
                        propertiesLength));
                CopyRegion(
                    PayloadSequence,
                    replacement.AsSpan(
                        topicLength + propertiesLength,
                        payloadLength));
                byte[] previous = _slabBuffer;
                SetOwnedRegions(
                    replacement,
                    topicLength,
                    propertiesLength,
                    payloadLength);
                _slabBuffer = replacement;
                _isBorrowed = false;
                ReturnSlab(previous);
            }
            catch
            {
                ReturnSlab(replacement);
                throw;
            }
        }
    }

    public void Retain()
    {
        Interlocked.Increment(ref _referenceCount);
    }

    public void Release()
    {
        int references = Interlocked.Decrement(ref _referenceCount);
        if (references > 0)
        {
            return;
        }

        ObjectDisposedException.ThrowIf(references < 0, this);
        byte[] slab = _slabBuffer;
        Reset();
        ReturnSlab(slab);
        LeasePool.Enqueue(this);
    }

    public void Dispose()
    {
        Release();
    }

    private static void CopyRegion(
        ReadOnlySequence<byte> source,
        Span<byte> destination)
    {
        if (source.Length > 0)
        {
            source.CopyTo(destination);
        }
    }

    private static MqttZeroMessageLease RentLease()
    {
        return LeasePool.TryDequeue(out MqttZeroMessageLease? lease)
            ? lease
            : new MqttZeroMessageLease();
    }

    private void InitializeBorrowed(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        MqttZeroBufferPool bufferPool)
    {
        _bufferPool = bufferPool;
        _isBorrowed = true;
        try
        {
            TopicSequence = topic;
            PropertiesSequence = properties;
            PayloadSequence = payload;
            int temporaryLength = checked(
                (int)topic.Length + (int)properties.Length);
            if (!topic.IsSingleSegment ||
                !properties.IsSingleSegment)
            {
                _slabBuffer = RentSlab(temporaryLength);
                byte[] slab = _slabBuffer;
                CopyRegion(
                    topic,
                    slab.AsSpan(0, (int)topic.Length));
                CopyRegion(
                    properties,
                    slab.AsSpan(
                        (int)topic.Length,
                        (int)properties.Length));
                Topic = slab.AsMemory(0, (int)topic.Length);
                Properties = slab.AsMemory(
                    (int)topic.Length,
                    (int)properties.Length);
                TopicSequence = new ReadOnlySequence<byte>(Topic);
                PropertiesSequence = new ReadOnlySequence<byte>(
                    Properties);
            }
            else
            {
                Topic = topic.First;
                Properties = properties.First;
            }

            Payload = payload.IsSingleSegment
                ? payload.First
                : ReadOnlyMemory<byte>.Empty;
            SetMetadata(
                qualityOfServiceLevel,
                retain,
                messageExpiryInterval,
                subscriptionIdentifier);
        }
        catch
        {
            byte[] slab = _slabBuffer;
            Reset();
            ReturnSlab(slab);
            throw;
        }
    }

    private void InitializeOwned(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        MqttZeroBufferPool bufferPool)
    {
        _bufferPool = bufferPool;
        int topicLength = checked((int)topic.Length);
        int propertiesLength = checked((int)properties.Length);
        int payloadLength = checked((int)payload.Length);
        int totalLength = checked(
            topicLength + propertiesLength + payloadLength);
        try
        {
            _slabBuffer = RentSlab(totalLength);
            CopyRegion(
                topic,
                _slabBuffer.AsSpan(0, topicLength));
            CopyRegion(
                properties,
                _slabBuffer.AsSpan(
                    topicLength,
                    propertiesLength));
            CopyRegion(
                payload,
                _slabBuffer.AsSpan(
                    topicLength + propertiesLength,
                    payloadLength));
            SetOwnedRegions(
                _slabBuffer,
                topicLength,
                propertiesLength,
                payloadLength);
            _isBorrowed = false;
            SetMetadata(
                qualityOfServiceLevel,
                retain,
                messageExpiryInterval,
                subscriptionIdentifier);
        }
        catch
        {
            byte[] slab = _slabBuffer;
            Reset();
            ReturnSlab(slab);
            throw;
        }
    }

    private byte[] RentSlab(int length)
    {
        return length == 0
            ? []
            : _bufferPool.Rent(length);
    }

    private void Reset()
    {
        Topic = ReadOnlyMemory<byte>.Empty;
        Properties = ReadOnlyMemory<byte>.Empty;
        Payload = ReadOnlyMemory<byte>.Empty;
        TopicSequence = ReadOnlySequence<byte>.Empty;
        PropertiesSequence = ReadOnlySequence<byte>.Empty;
        PayloadSequence = ReadOnlySequence<byte>.Empty;
        _isBorrowed = false;
        _topicOffset = 0;
        _topicLength = 0;
        _propertiesOffset = 0;
        _propertiesLength = 0;
        _payloadOffset = 0;
        _payloadLength = 0;
        _slabBuffer = [];
        MessageExpiryInterval = null;
        SubscriptionIdentifier = null;
        ExpiresAt = null;
        PacketIdentifier = 0;
    }

    private void ReturnSlab(byte[] slab)
    {
        if (slab.Length > 0)
        {
            _bufferPool.Return(slab);
        }
    }

    private void SetMetadata(
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier)
    {
        QualityOfServiceLevel = qualityOfServiceLevel;
        RetainFlag = retain;
        MessageExpiryInterval = messageExpiryInterval;
        SubscriptionIdentifier = subscriptionIdentifier;
        ExpiresAt = null;
        PacketIdentifier = 0;
        _referenceCount = 1;
    }

    private void SetOwnedRegions(
        byte[] slab,
        int topicLength,
        int propertiesLength,
        int payloadLength)
    {
        _topicOffset = 0;
        _topicLength = topicLength;
        _propertiesOffset = topicLength;
        _propertiesLength = propertiesLength;
        _payloadOffset = topicLength + propertiesLength;
        _payloadLength = payloadLength;
        Topic = slab.AsMemory(_topicOffset, _topicLength);
        Properties = slab.AsMemory(
            _propertiesOffset,
            _propertiesLength);
        Payload = slab.AsMemory(_payloadOffset, _payloadLength);
        TopicSequence = new ReadOnlySequence<byte>(Topic);
        PropertiesSequence = new ReadOnlySequence<byte>(Properties);
        PayloadSequence = new ReadOnlySequence<byte>(Payload);
    }
}
