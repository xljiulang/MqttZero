using MqttZero.Memory;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Sessions;

/// <summary>
/// 队列中的一条出站消息及其状态。
/// </summary>
internal sealed class MqttZeroQueuedMessage
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroQueuedMessage>
        MessagePool = new(
            MqttZeroBoundedObjectPool<MqttZeroQueuedMessage>.
                DefaultCapacity);

    private MqttZeroMessageLease _lease = null!;
    private bool _isHeld;
    private uint[] _subscriptionIdentifierBuffer = [];

    /// <summary>
    /// 队列下一个。
    /// </summary>
    internal MqttZeroQueuedMessage? QueueNext { get; set; }

    /// <summary>
    /// 队列前一个。
    /// </summary>
    internal MqttZeroQueuedMessage? QueuePrevious { get; set; }

    /// <summary>
    /// Qos0下一个。
    /// </summary>
    internal MqttZeroQueuedMessage? Qos0Next { get; set; }

    /// <summary>
    /// Qos0前一个。
    /// </summary>
    internal MqttZeroQueuedMessage? Qos0Previous { get; set; }

    /// <summary>
    /// 队列版本。
    /// </summary>
    internal long QueueVersion { get; set; }

    /// <summary>
    /// 判断是否Queued。
    /// </summary>
    internal bool IsQueued { get; set; }

    /// <summary>
    /// 判断是否Qos0Queued。
    /// </summary>
    internal bool IsQos0Queued { get; set; }

    /// <summary>
    /// 初始化 MqttZeroQueuedMessage 的新实例。
    /// </summary>
    private MqttZeroQueuedMessage()
    {
    }

    internal static int PoolCountForTest
    {
        get
        {
            return MessagePool.Count;
        }
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

    /// <summary>
    /// 服务质量ServiceLevel。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; private set; }

    /// <summary>
    /// 保留。
    /// </summary>
    public bool Retain { get; private set; }

    /// <summary>
    /// 订阅标识符。
    /// </summary>
    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; private set; }

    /// <summary>
    /// ExpiresAt 的值。
    /// </summary>
    public DateTimeOffset? ExpiresAt { get; private set; }

    /// <summary>
    /// 报文标识符。
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// 状态。
    /// </summary>
    public MqttZeroOutboundState State { get; private set; }

    /// <summary>
    /// 消息Id。
    /// </summary>
    public long MessageId { get; private set; }

    /// <summary>
    /// Encoded长度。
    /// </summary>
    public int EncodedLength { get; private set; }

    private uint _encodedLengthMaximumPacketSize;

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 租用With标识符。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroQueuedMessage RentWithIdentifiers(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        DateTimeOffset? expiresAt)
    {
        if (!MessagePool.TryRent(out MqttZeroQueuedMessage? message) ||
            message is null)
        {
            message = new MqttZeroQueuedMessage();
        }

        if (message._isHeld)
        {
            throw new InvalidOperationException(
                "The queued message was rented while already held.");
        }

        message._isHeld = true;
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

    /// <summary>
    /// 租用WithCopied标识符。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 设置AwaitPubComp。
    /// </summary>
    public void SetAwaitPubComp()
    {
        State = MqttZeroOutboundState.AwaitPubComp;
    }

    /// <summary>
    /// 设置AwaitPubRec。
    /// </summary>
    public void SetAwaitPubRec()
    {
        State = MqttZeroOutboundState.AwaitPubRec;
    }

    /// <summary>
    /// 设置消息Id。
    /// </summary>
    /// <param name="messageId">消息Id。</param>
    public void SetMessageId(long messageId)
    {
        MessageId = messageId;
    }

    /// <summary>
    /// 设置Encoded长度。
    /// </summary>
    /// <param name="encodedLength">encoded长度。</param>
    /// <param name="maximumPacketSize">最大报文大小。</param>
    public void SetEncodedLength(int encodedLength, uint maximumPacketSize)
    {
        EncodedLength = encodedLength;
        _encodedLengthMaximumPacketSize = maximumPacketSize;
    }

    /// <summary>
    /// 判断是否Encoded长度Valid。
    /// </summary>
    /// <param name="maximumPacketSize">最大报文大小。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool IsEncodedLengthValidFor(uint maximumPacketSize)
    {
        return EncodedLength > 0 &&
            _encodedLengthMaximumPacketSize == maximumPacketSize;
    }

    /// <summary>
    /// 恢复。
    /// </summary>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="state">状态。</param>
    public void Restore(ushort packetIdentifier, MqttZeroOutboundState state)
    {
        PacketIdentifier = packetIdentifier;
        State = state;
    }

    /// <summary>
    /// 恢复。
    /// </summary>
    /// <param name="messageId">消息Id。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="state">状态。</param>
    public void Restore(long messageId, ushort packetIdentifier, MqttZeroOutboundState state)
    {
        MessageId = messageId;
        PacketIdentifier = packetIdentifier;
        State = state;
    }

    /// <summary>
    /// 判断是否Expired。
    /// </summary>
    /// <param name="now">当前时间。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool IsExpired(DateTimeOffset now)
    {
        return ExpiresAt.HasValue && ExpiresAt.Value <= now;
    }

    /// <summary>
    /// 创建报文。
    /// </summary>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 创建View。
    /// </summary>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 释放。
    /// </summary>
    public void Release()
    {
        ObjectDisposedException.ThrowIf(!_isHeld || _lease is null, this);
        _isHeld = false;
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
        _ = MessagePool.Return(this);
    }
}
