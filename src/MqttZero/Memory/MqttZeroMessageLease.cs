using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Memory;

/// <summary>
/// 引用计数的消息租约，共享主题、属性与负载。
/// </summary>
internal sealed class MqttZeroMessageLease : IDisposable
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroMessageLease> LeasePool = new(MqttZeroBoundedObjectPool<MqttZeroMessageLease>.DefaultCapacity);

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

    /// <summary>
    /// 初始化 MqttZeroMessageLease 的新实例。
    /// </summary>
    private MqttZeroMessageLease()
    {
    }

    /// <summary>
    /// 主题。
    /// </summary>
    public ReadOnlyMemory<byte> Topic { get; private set; }

    /// <summary>
    /// 属性。
    /// </summary>
    public ReadOnlyMemory<byte> Properties { get; private set; }

    /// <summary>
    /// 获取连续的负载内存块。借用态的多段负载只能通过
    /// <see cref="PayloadSequence"/> 读取，直到 <see cref="EnsureOwned"/> 把它提升为自有内存。
    /// </summary>
    public ReadOnlyMemory<byte> Payload
    {
        get
        {
            return PayloadSequence.IsSingleSegment
                ? PayloadSequence.First
                : ReadOnlyMemory<byte>.Empty;
        }
    }

    /// <summary>
    /// 主题序列。
    /// </summary>
    public ReadOnlySequence<byte> TopicSequence { get; private set; }

    /// <summary>
    /// 属性序列。
    /// </summary>
    public ReadOnlySequence<byte> PropertiesSequence { get; private set; }

    /// <summary>
    /// 负载序列。
    /// </summary>
    public ReadOnlySequence<byte> PayloadSequence { get; private set; }

    /// <summary>
    /// IsBorrowed 的值。
    /// </summary>
    public bool IsBorrowed
    {
        get
        {
            return _isBorrowed;
        }
    }

    /// <summary>
    /// 服务质量ServiceLevel。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; private set; }

    /// <summary>
    /// 保留标志。
    /// </summary>
    public bool RetainFlag { get; private set; }

    /// <summary>
    /// 消息Expiry间隔。
    /// </summary>
    public uint? MessageExpiryInterval { get; private set; }

    /// <summary>
    /// 订阅标识符。
    /// </summary>
    public uint? SubscriptionIdentifier { get; private set; }

    /// <summary>
    /// ExpiresAt 的值。
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; private set; }

    /// <summary>
    /// 报文标识符。
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// PoolCountForTest 的值。
    /// </summary>
    internal static int PoolCountForTest
    {
        get
        {
            return LeasePool.Count;
        }
    }

    /// <summary>
    /// 复制From。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="properties">属性。</param>
    /// <param name="payload">负载。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="messageExpiryInterval">消息Expiry间隔。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="bufferPool">缓冲区池。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 执行 BorrowFrom 对应的处理。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="properties">属性。</param>
    /// <param name="payload">负载。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="messageExpiryInterval">消息Expiry间隔。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="bufferPool">缓冲区池。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 创建OwnedCopy。
    /// </summary>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 设置ExpiresAt。
    /// </summary>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    public void SetExpiresAt(DateTimeOffset? expiresAt)
    {
        ExpiresAt = expiresAt;
    }

    /// <summary>
    /// 确保Owned。
    /// </summary>
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

    /// <summary>
    /// 保留。
    /// </summary>
    public void Retain()
    {
        Interlocked.Increment(ref _referenceCount);
    }

    /// <summary>
    /// 释放。
    /// </summary>
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
        _ = LeasePool.Return(this);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Dispose()
    {
        Release();
    }

    /// <summary>
    /// 复制Region。
    /// </summary>
    /// <param name="source">来源。</param>
    /// <param name="destination">对应的 destination。</param>
    private static void CopyRegion(
        ReadOnlySequence<byte> source,
        Span<byte> destination)
    {
        if (source.Length > 0)
        {
            source.CopyTo(destination);
        }
    }

    /// <summary>
    /// 租用租约。
    /// </summary>
    /// <returns>对应的结果。</returns>
    private static MqttZeroMessageLease RentLease()
    {
        return LeasePool.TryRent(out MqttZeroMessageLease? lease)
            ? lease
            : new MqttZeroMessageLease();
    }

    /// <summary>
    /// 初始化Borrowed。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="properties">属性。</param>
    /// <param name="payload">负载。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="messageExpiryInterval">消息Expiry间隔。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="bufferPool">缓冲区池。</param>
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

    /// <summary>
    /// 初始化Owned。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="properties">属性。</param>
    /// <param name="payload">负载。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="messageExpiryInterval">消息Expiry间隔。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="bufferPool">缓冲区池。</param>
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

    /// <summary>
    /// 租用内存块。
    /// </summary>
    /// <param name="length">长度。</param>
    /// <returns>字节数组。</returns>
    private byte[] RentSlab(int length)
    {
        return length == 0
            ? []
            : _bufferPool.Rent(length);
    }

    /// <summary>
    /// 重置。
    /// </summary>
    private void Reset()
    {
        Topic = ReadOnlyMemory<byte>.Empty;
        Properties = ReadOnlyMemory<byte>.Empty;
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

    /// <summary>
    /// 归还内存块。
    /// </summary>
    /// <param name="slab">内存块。</param>
    private void ReturnSlab(byte[] slab)
    {
        if (slab.Length > 0)
        {
            _bufferPool.Return(slab);
        }
    }

    /// <summary>
    /// 设置Metadata。
    /// </summary>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="messageExpiryInterval">消息Expiry间隔。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
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

    /// <summary>
    /// 设置OwnedRegions。
    /// </summary>
    /// <param name="slab">内存块。</param>
    /// <param name="topicLength">主题长度。</param>
    /// <param name="propertiesLength">属性长度。</param>
    /// <param name="payloadLength">负载长度。</param>
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
        TopicSequence = new ReadOnlySequence<byte>(Topic);
        PropertiesSequence = new ReadOnlySequence<byte>(Properties);
        PayloadSequence = new ReadOnlySequence<byte>(
            slab.AsMemory(_payloadOffset, _payloadLength));
    }
}
