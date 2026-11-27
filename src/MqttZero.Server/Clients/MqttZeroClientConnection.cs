using MqttZero.Memory;
using MqttZero.Server.Observers;
using MqttZero.Server.Egress;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net;

namespace MqttZero.Server.Clients;

/// <summary>
/// 一条 MQTT 客户端连接的读循环、写门与出站协调。
/// </summary>
internal sealed class MqttZeroClientConnection : IAsyncDisposable
{
    private static long NextConnectionId;
    private static readonly TimeSpan WriteGateTimeout =
        TimeSpan.FromSeconds(2);

    private readonly MqttZeroPacketWriter _bufferWriter = new();
    private readonly MqttZeroBufferPool _bufferPool;
    private readonly CancellationTokenSource _closed = new();
    private readonly MqttZeroEgressControlFramePool _controlFrames;
    private readonly PipeWriter _writer;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly DateTimeOffset _connectedAt;
    private MqttZeroSessionEgress? _egress;
    private MqttZeroTopicAliasTable? _inboundTopicAliases;
    private readonly TimeProvider _timeProvider;
    private int _disposed;
    private int _pendingControlWrites;
    private int _writersInFlight;
    private long _lastPacketReceivedTicks;
    private long _packetsReceived;
    private long _packetsSent;
    private long _bytesReceived;
    private long _bytesSent;
    private long _messagesReceived;
    private long _messagesSent;

    /// <summary>
    /// 初始化 MqttZeroClientConnection 的新实例。
    /// </summary>
    /// <param name="writer">写入器。</param>
    /// <param name="remoteEndPoint">远端EndPoint。</param>
    /// <param name="isSecure">对应的 isSecure。</param>
    /// <param name="isWebSocket">对应的 isWebSocket。</param>
    /// <param name="keepAliveSeconds">对应的 keepAliveSeconds。</param>
    /// <param name="receiveMaximum">receive最大。</param>
    /// <param name="maximumPacketSize">最大报文大小。</param>
    /// <param name="timeProvider">时间Provider。</param>
    /// <param name="bufferPool">缓冲区池。</param>
    /// <param name="allowPacketFragmentation">allow报文Fragmentation。</param>
    /// <param name="requestProblemInformation">对应的 requestProblemInformation。</param>
    /// <param name="requestResponseInformation">对应的 requestResponseInformation。</param>
    /// <param name="shardId">分片Id。</param>
    public MqttZeroClientConnection(
        PipeWriter writer,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        ushort keepAliveSeconds,
        ushort receiveMaximum,
        uint maximumPacketSize,
        TimeProvider timeProvider,
        MqttZeroBufferPool? bufferPool = null,
        bool allowPacketFragmentation = true,
        bool requestProblemInformation = true,
        bool requestResponseInformation = false,
        int shardId = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        _bufferPool = bufferPool ?? MqttZeroBufferPool.Shared;
        _controlFrames = new MqttZeroEgressControlFramePool(_bufferPool);
        ConnectionId = Interlocked.Increment(ref NextConnectionId);
        ShardId = shardId;
        _writer = writer;
        _timeProvider = timeProvider;
        RemoteEndPoint = remoteEndPoint;
        IsSecure = isSecure;
        IsWebSocket = isWebSocket;
        KeepAliveSeconds = keepAliveSeconds;
        ReceiveMaximum = receiveMaximum;
        MaximumPacketSize = maximumPacketSize;
        AllowPacketFragmentation = allowPacketFragmentation;
        RequestProblemInformation = requestProblemInformation;
        RequestResponseInformation = requestResponseInformation;
        Volatile.Write(
            ref _lastPacketReceivedTicks,
            timeProvider.GetUtcNow().UtcTicks);
        _connectedAt = LastPacketReceivedAt;
    }

    /// <summary>
    /// 远端EndPoint。
    /// </summary>
    public EndPoint? RemoteEndPoint { get; }

    /// <summary>
    /// 连接Id。
    /// </summary>
    internal long ConnectionId { get; }

    /// <summary>
    /// 分片Id。
    /// </summary>
    internal int ShardId { get; set; }

    /// <summary>
    /// Egress 的值。
    /// </summary>
    internal MqttZeroSessionEgress? Egress
    {
        get
        {
            return _egress;
        }
    }

    /// <summary>
    /// 挂接出站。
    /// </summary>
    /// <param name="egress">出站。</param>
    internal void AttachEgress(MqttZeroSessionEgress egress)
    {
        ArgumentNullException.ThrowIfNull(egress);
        _egress = egress;
    }

    /// <summary>
    /// 判断是否Secure。
    /// </summary>
    public bool IsSecure { get; }

    /// <summary>
    /// 判断是否WebSocket。
    /// </summary>
    public bool IsWebSocket { get; }

    /// <summary>
    /// KeepAliveSeconds 的值。
    /// </summary>
    public ushort KeepAliveSeconds { get; }

    /// <summary>
    /// Receive最大。
    /// </summary>
    public ushort ReceiveMaximum { get; }

    /// <summary>
    /// 最大报文大小。
    /// </summary>
    public uint MaximumPacketSize { get; }

    /// <summary>
    /// Allow报文Fragmentation。
    /// </summary>
    public bool AllowPacketFragmentation { get; }

    /// <summary>
    /// RequestProblemInformation 的值。
    /// </summary>
    public bool RequestProblemInformation { get; }

    /// <summary>
    /// RequestResponseInformation 的值。
    /// </summary>
    public bool RequestResponseInformation { get; }

    /// <summary>
    /// 客户端Info。
    /// </summary>
    public MqttZeroClientConnectionInfo ClientInfo { get; set; } = null!;

    /// <summary>
    /// HasPendingControlWrites 的值。
    /// </summary>
    internal bool HasPendingControlWrites
    {
        get
        {
            return Volatile.Read(ref _pendingControlWrites) > 0;
        }
    }

    /// <summary>
    /// 判断是否Connected客户端Counted。
    /// </summary>
    public bool IsConnectedClientCounted { get; set; }

    /// <summary>
    /// Connected客户端。
    /// </summary>
    public MqttZeroConnectedClient ConnectedClient { get; set; } = null!;

    /// <summary>
    /// 断开类型。
    /// </summary>
    public MqttZeroDisconnectType DisconnectType { get; set; } = MqttZeroDisconnectType.ConnectionLost;

    /// <summary>
    /// 关闭原因代码。
    /// </summary>
    public MqttReasonCode? CloseReasonCode { get; private set; }

    /// <summary>
    /// Gets the UTC time of the last received packet. The underlying value is
    /// stored as one tick value so readers never observe a torn timestamp.
    /// </summary>
    public DateTimeOffset LastPacketReceivedAt
    {
        get
        {
            return new DateTimeOffset(
                Volatile.Read(ref _lastPacketReceivedTicks),
                TimeSpan.Zero);
        }
    }

    /// <summary>
    /// 判断是否ReadingIncomplete报文。
    /// </summary>
    public bool IsReadingIncompletePacket { get; set; }

    /// <summary>
    /// IsClosed 的值。
    /// </summary>
    internal bool IsClosed
    {
        get
        {
            return Volatile.Read(ref _disposed) != 0 ||
                _closed.IsCancellationRequested;
        }
    }

    /// <summary>
    /// ClosedToken 的值。
    /// </summary>
    public CancellationToken ClosedToken
    {
        get
        {
            return _closed.Token;
        }
    }

    /// <summary>
    /// Mark报文Received。
    /// </summary>
    /// <param name="timeProvider">时间Provider。</param>
    public void MarkPacketReceived(TimeProvider timeProvider)
    {
        Volatile.Write(
            ref _lastPacketReceivedTicks,
            timeProvider.GetUtcNow().UtcTicks);
        Interlocked.Increment(ref _packetsReceived);
    }

    /// <summary>
    /// Mark报文Received。
    /// </summary>
    /// <param name="timeProvider">时间Provider。</param>
    /// <param name="byteCount">字节数量。</param>
    /// <param name="applicationMessage">application消息。</param>
    public void MarkPacketReceived(
        TimeProvider timeProvider,
        long byteCount,
        bool applicationMessage)
    {
        Volatile.Write(
            ref _lastPacketReceivedTicks,
            timeProvider.GetUtcNow().UtcTicks);
        Interlocked.Increment(ref _packetsReceived);
        Interlocked.Add(ref _bytesReceived, byteCount);
        if (applicationMessage)
        {
            Interlocked.Increment(ref _messagesReceived);
        }
    }

    /// <summary>
    /// 获取统计。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroClientStatistics GetStatistics()
    {
        return new MqttZeroClientStatistics
        {
            ConnectedAt = _connectedAt,
            LastPacketReceivedAt = LastPacketReceivedAt,
            PacketsReceived = Interlocked.Read(ref _packetsReceived),
            PacketsSent = Interlocked.Read(ref _packetsSent),
            BytesReceived = Interlocked.Read(ref _bytesReceived),
            BytesSent = Interlocked.Read(ref _bytesSent),
            MessagesReceived = Interlocked.Read(ref _messagesReceived),
            MessagesSent = Interlocked.Read(ref _messagesSent)
        };
    }

    /// <summary>
    /// 设置入站主题别名。
    /// </summary>
    /// <param name="alias">别名。</param>
    /// <param name="topic">主题。</param>
    public void SetInboundTopicAlias(ushort alias, ReadOnlySequence<byte> topic)
    {
        (_inboundTopicAliases ??= new MqttZeroTopicAliasTable(
            _bufferPool)).Set(alias, topic);
    }

    /// <summary>
    /// 尝试Get入站主题别名。
    /// </summary>
    /// <param name="alias">别名。</param>
    /// <param name="topic">主题。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public bool TryGetInboundTopicAlias(ushort alias, out ReadOnlyMemory<byte> topic)
    {
        if (_inboundTopicAliases != null)
        {
            return _inboundTopicAliases.TryGet(alias, out topic);
        }

        topic = ReadOnlyMemory<byte>.Empty;
        return false;
    }

    /// <summary>
    /// 发送。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken)
    {
        MqttZeroEgressControlFrame? frame;
        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            _bufferWriter.Clear();
            MqttPacketCodec.Encode(packet, _bufferWriter);
            if (_bufferWriter.WrittenCount > MaximumPacketSize)
            {
                throw new InvalidOperationException(
                    "The outbound MQTT packet exceeds the client maximum packet size.");
            }

            frame = TryRentEgressControlFrame(_bufferWriter.WrittenCount);
            if (frame != null)
            {
                Span<byte> destination = frame.Writer.GetSpan(
                    _bufferWriter.WrittenCount);
                _bufferWriter.WrittenMemory.Span.CopyTo(destination);
                frame.Writer.Advance(_bufferWriter.WrittenCount);
            }
            else
            {
                BeginControlWrite();
                try
                {
                    _ = await _writer.WriteAsync(
                        _bufferWriter.WrittenMemory,
                        cancellationToken).ConfigureAwait(false);
                    _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                    MarkPacketSent(
                        _bufferWriter.WrittenCount,
                        packet is MqttPublishPacket);
                }
                finally
                {
                    EndControlWrite();
                }

                return;
            }
        }

        await PostControlFrameAsync(frame, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 写入Encoded控制批次。
    /// </summary>
    /// <param name="frames">帧。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    internal async ValueTask WriteEncodedControlBatchAsync(
        IReadOnlyList<MqttZeroEgressControlFrame> frames,
        CancellationToken cancellationToken)
    {
        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            BeginControlWrite();
            try
            {
                for (int index = 0; index < frames.Count; index++)
                {
                    _ = await _writer.WriteAsync(
                        frames[index].EncodedMemory,
                        cancellationToken).ConfigureAwait(false);
                }

                _ = await _writer.FlushAsync(cancellationToken)
                    .ConfigureAwait(false);
                for (int index = 0; index < frames.Count; index++)
                {
                    MarkPacketSent(frames[index].EncodedLength, false);
                }
            }
            finally
            {
                EndControlWrite();
            }
        }
    }

    /// <summary>
    /// 发送发布。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendPublishAsync(
        MqttZeroQueuedMessage message,
        bool duplicate,
        CancellationToken cancellationToken)
    {
        await WritePublishPacketAsync(
            message.Lease.TopicSequence,
            message.Lease.PropertiesSequence,
            message.Lease.PayloadSequence,
            message.QualityOfServiceLevel,
            message.PacketIdentifier,
            duplicate,
            message.Retain,
            message.ExpiresAt,
            message.SubscriptionIdentifiers,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 发送发布批次。
    /// </summary>
    /// <param name="messages">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <returns>表示异步操作的任务。</returns>
    internal async ValueTask SendPublishBatchAsync(
        IReadOnlyList<MqttZeroQueuedMessage> messages,
        CancellationToken cancellationToken,
        bool duplicate = false)
    {
        if (messages.Count == 0)
        {
            return;
        }

        if (!AllowPacketFragmentation)
        {
            for (int index = 0; index < messages.Count; index++)
            {
                await SendPublishAsync(
                    messages[index],
                    duplicate,
                    cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        int encodedBytes;
        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            encodedBytes = WritePublishBatch(messages, duplicate);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        MarkPublishBatchSent(messages.Count, encodedBytes);
    }

    /// <summary>
    /// 编码发布批次。
    /// </summary>
    /// <param name="messages">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    internal async ValueTask<int> EncodePublishBatchAsync(
        IReadOnlyList<MqttZeroQueuedMessage> messages,
        CancellationToken cancellationToken)
    {
        if (!AllowPacketFragmentation || messages.Count == 0)
        {
            return 0;
        }

        int encodedBytes;
        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            encodedBytes = WritePublishBatch(messages);
        }

        MarkPublishBatchSent(messages.Count, encodedBytes);
        return encodedBytes;
    }

    /// <summary>
    /// 写入发布批次。
    /// </summary>
    /// <param name="messages">消息。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <returns>计算得到的数值。</returns>
    private int WritePublishBatch(
        IReadOnlyList<MqttZeroQueuedMessage> messages,
        bool duplicate = false)
    {
        int encodedBytes = 0;
        for (int index = 0; index < messages.Count; index++)
        {
            MqttZeroQueuedMessage message = messages[index];
            MqttZeroPublishView view = new(
                message.Lease.TopicSequence,
                message.Lease.PropertiesSequence,
                message.Lease.PayloadSequence,
                message.QualityOfServiceLevel,
                message.PacketIdentifier,
                duplicate,
                message.Retain,
                GetRemainingMessageExpiry(message.ExpiresAt),
                null,
                message.SubscriptionIdentifiers,
                null);
            int encodedLength =
                message.IsEncodedLengthValidFor(MaximumPacketSize)
                    ? message.EncodedLength
                    : MqttPublishPacketCodec.GetEncodedLength(view);
            if (encodedLength > MaximumPacketSize)
            {
                throw new InvalidOperationException(
                    "The outbound MQTT packet exceeds the client maximum packet size.");
            }

            if (message.Lease.Payload.Length < 64 * 1024 &&
                message.Lease.Properties.IsEmpty)
            {
                _ = MqttPublishPacketCodec.EncodeToWriter(
                    view,
                    _bufferWriter,
                    _writer);
            }
            else
            {
                MqttPublishPacketCodec.Encode(view, _writer);
            }

            encodedBytes += encodedLength;
        }

        return encodedBytes;
    }

    /// <summary>
    /// Mark发布批次Sent。
    /// </summary>
    /// <param name="messageCount">消息数量。</param>
    /// <param name="encodedBytes">encoded字节数。</param>
    private void MarkPublishBatchSent(
        int messageCount,
        int encodedBytes)
    {
        Interlocked.Add(ref _packetsSent, messageCount);
        Interlocked.Add(ref _bytesSent, encodedBytes);
        Interlocked.Add(ref _messagesSent, messageCount);
    }

    /// <summary>
    /// 刷出Output。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    internal async ValueTask<FlushResult> FlushOutputAsync(
        CancellationToken cancellationToken)
    {
        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            return await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 发送发布。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendPublishAsync(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? subscriptionIdentifier,
        ushort packetIdentifier,
        bool duplicate,
        CancellationToken cancellationToken)
    {
        await SendPublishWithIdentifiersAsync(
            lease,
            qualityOfServiceLevel,
            retain,
            subscriptionIdentifier.HasValue
                ? new[] { subscriptionIdentifier.Value }
                : ReadOnlyMemory<uint>.Empty,
            packetIdentifier,
            duplicate,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 发送发布With标识符。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendPublishWithIdentifiersAsync(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        ushort packetIdentifier,
        bool duplicate,
        CancellationToken cancellationToken)
    {
        await WritePublishPacketAsync(
            lease.TopicSequence,
            lease.PropertiesSequence,
            lease.PayloadSequence,
            qualityOfServiceLevel,
            packetIdentifier,
            duplicate,
            retain,
            lease.ExpiresAt,
            subscriptionIdentifiers,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Exceeds最大报文大小。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    internal bool ExceedsMaximumPacketSize(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers)
    {
        return GetPublishEncodedLength(
            lease,
            qualityOfServiceLevel,
            retain,
            subscriptionIdentifiers) > MaximumPacketSize;
    }

    /// <summary>
    /// 获取发布Encoded长度。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <returns>计算得到的数值。</returns>
    internal int GetPublishEncodedLength(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers)
    {
        return GetPublishEncodedLength(
            lease.TopicSequence,
            lease.PropertiesSequence,
            lease.PayloadSequence,
            qualityOfServiceLevel,
            0,
            false,
            retain,
            GetRemainingMessageExpiry(lease.ExpiresAt),
            subscriptionIdentifiers);
    }

    /// <summary>
    /// 发送Pub确认。
    /// </summary>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SendPubAckAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        return SendAckDirectAsync(
            MqttPacketHeader.GetFirstByte(MqttPacketType.PubAck),
            packetIdentifier,
            reasonCode,
            cancellationToken,
            waitForWrite: false);
    }

    /// <summary>
    /// 发送PubRec。
    /// </summary>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SendPubRecAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        return SendAckDirectAsync(
            MqttPacketHeader.GetFirstByte(MqttPacketType.PubRec),
            packetIdentifier,
            reasonCode,
            cancellationToken);
    }

    /// <summary>
    /// 发送PubRel。
    /// </summary>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SendPubRelAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        return SendAckDirectAsync(
            MqttPacketHeader.GetFirstByte(MqttPacketType.PubRel),
            packetIdentifier,
            reasonCode,
            cancellationToken,
            coalesceOnShard: true);
    }

    /// <summary>
    /// 发送PubRelWithoutWaiting。
    /// </summary>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    internal ValueTask SendPubRelWithoutWaitingAsync(
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        return SendAckDirectAsync(
            MqttPacketHeader.GetFirstByte(MqttPacketType.PubRel),
            packetIdentifier,
            MqttReasonCode.Success,
            cancellationToken,
            waitForWrite: false,
            coalesceOnShard: true);
    }

    /// <summary>
    /// 发送PubComp。
    /// </summary>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SendPubCompAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        return SendAckDirectAsync(
            MqttPacketHeader.GetFirstByte(MqttPacketType.PubComp),
            packetIdentifier,
            reasonCode,
            cancellationToken,
            coalesceOnShard: true);
    }

    /// <summary>
    /// 发送PingResp。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendPingRespAsync(CancellationToken cancellationToken)
    {
        if (MaximumPacketSize < 2)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(2);
        if (frame != null)
        {
            Span<byte> destination = frame.Writer.GetSpan(2);
            destination[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.PingResp);
            destination[1] = 0;
            frame.Writer.Advance(2);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            BeginControlWrite();
            try
            {
                Span<byte> header = _writer.GetSpan(2);
                header[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.PingResp);
                header[1] = 0;
                _writer.Advance(2);
                _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                MarkPacketSent(2, false);
            }
            finally
            {
                EndControlWrite();
            }
        }
    }

    /// <summary>
    /// 发送Sub确认。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendSubAckAsync(
        MqttSubAckPacket packet,
        CancellationToken cancellationToken)
    {
        MqttZeroControlPacketSize size = MqttZeroControlPacketSize.ForReasonCodeList(
            Mqtt5PropertyWriter.GetReasonPropertiesLength(
                packet.ReasonString,
                packet.UserProperties),
            packet.ReasonCodes.Length);
        if (size.EncodedLength > MaximumPacketSize &&
            (packet.ReasonString != null || packet.UserProperties.Count > 0))
        {
            packet.ReasonString = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
            size = MqttZeroControlPacketSize.ForReasonCodeList(
                0,
                packet.ReasonCodes.Length);
        }

        await SendControlPacketAsync(packet, size, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 发送Unsub确认。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendUnsubAckAsync(
        MqttUnsubAckPacket packet,
        CancellationToken cancellationToken)
    {
        MqttZeroControlPacketSize size = MqttZeroControlPacketSize.ForReasonCodeList(
            Mqtt5PropertyWriter.GetReasonPropertiesLength(
                packet.ReasonString,
                packet.UserProperties),
            packet.ReasonCodes.Length);
        if (size.EncodedLength > MaximumPacketSize &&
            (packet.ReasonString != null || packet.UserProperties.Count > 0))
        {
            packet.ReasonString = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
            size = MqttZeroControlPacketSize.ForReasonCodeList(
                0,
                packet.ReasonCodes.Length);
        }

        await SendControlPacketAsync(packet, size, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 发送Conn确认。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendConnAckAsync(
        MqttConnAckPacket packet,
        CancellationToken cancellationToken)
    {
        if (!RequestProblemInformation)
        {
            packet.ReasonString = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
        }

        MqttZeroControlPacketSize size = MqttZeroControlPacketSize.ForConnAck(
            Mqtt5PropertyWriter.GetConnAckPropertiesLength(packet));
        if (size.EncodedLength > MaximumPacketSize &&
            (packet.ReasonString != null || packet.UserProperties.Count > 0))
        {
            packet.ReasonString = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
            size = MqttZeroControlPacketSize.ForConnAck(
                Mqtt5PropertyWriter.GetConnAckPropertiesLength(packet));
        }

        if (size.EncodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(size.EncodedLength);
        if (frame != null)
        {
            MqttPacketCodec.Encode(packet, frame.Writer);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            BeginControlWrite();
            try
            {
                Span<byte> fixedHeader = _writer.GetSpan(5);
                fixedHeader[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.ConnAck);
                int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(
                    fixedHeader[1..],
                    size.BodyLength);
                _writer.Advance(1 + lengthBytes);
                Span<byte> acknowledgeHeader = _writer.GetSpan(2);
                acknowledgeHeader[0] = packet.SessionPresent ? (byte)1 : (byte)0;
                acknowledgeHeader[1] = (byte)packet.ReasonCode;
                _writer.Advance(2);
                Mqtt5PropertyWriter.WriteConnAckProperties(packet, _writer);
                _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                MarkPacketSent(size.EncodedLength, false);
            }
            finally
            {
                EndControlWrite();
            }
        }
    }

    /// <summary>
    /// 发送断开。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask SendDisconnectAsync(
        MqttDisconnectPacket packet,
        CancellationToken cancellationToken)
    {
        if (!RequestProblemInformation)
        {
            packet.ReasonString = null;
            packet.ServerReference = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
        }

        int propertiesLength =
            Mqtt5PropertyWriter.GetDisconnectPropertiesLength(packet);
        bool includeDetail =
            packet.ReasonCode != MqttReasonCode.Success ||
            propertiesLength > 0;
        MqttZeroControlPacketSize size = MqttZeroControlPacketSize.ForDisconnect(
            propertiesLength,
            includeDetail);
        if (size.EncodedLength > MaximumPacketSize &&
            (packet.ReasonString != null ||
                packet.ServerReference != null ||
                packet.UserProperties.Count > 0))
        {
            packet.ReasonString = null;
            packet.ServerReference = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
            propertiesLength =
                Mqtt5PropertyWriter.GetDisconnectPropertiesLength(packet);
            includeDetail =
                packet.ReasonCode != MqttReasonCode.Success ||
                propertiesLength > 0;
            size = MqttZeroControlPacketSize.ForDisconnect(
                propertiesLength,
                includeDetail);
        }

        if (size.EncodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(size.EncodedLength);
        if (frame != null)
        {
            MqttPacketCodec.Encode(packet, frame.Writer);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            BeginControlWrite();
            try
            {
                Span<byte> fixedHeader = _writer.GetSpan(5);
                fixedHeader[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.Disconnect);
                int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(
                    fixedHeader[1..],
                    size.BodyLength);
                _writer.Advance(1 + lengthBytes);
                if (includeDetail)
                {
                    _writer.GetSpan(1)[0] = (byte)packet.ReasonCode;
                    _writer.Advance(1);
                    Mqtt5PropertyWriter.WriteDisconnectProperties(packet, _writer);
                }

                _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                MarkPacketSent(size.EncodedLength, false);
            }
            finally
            {
                EndControlWrite();
            }
        }
    }

    /// <summary>
    /// 写入发布报文。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="properties">属性。</param>
    /// <param name="payload">负载。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <param name="retain">保留。</param>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask WritePublishPacketAsync(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        ushort packetIdentifier,
        bool duplicate,
        bool retain,
        DateTimeOffset? expiresAt,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        CancellationToken cancellationToken)
    {
        uint? remainingExpiry = GetRemainingMessageExpiry(expiresAt);
        int encodedLength = GetPublishEncodedLength(
            topic,
            properties,
            payload,
            qualityOfServiceLevel,
            packetIdentifier,
            duplicate,
            retain,
            remainingExpiry,
            subscriptionIdentifiers);
        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            MqttZeroPublishView view = new(
                topic,
                properties,
                payload,
                qualityOfServiceLevel,
                packetIdentifier,
                duplicate,
                retain,
                remainingExpiry,
                null,
                subscriptionIdentifiers,
                null);
            if (AllowPacketFragmentation)
            {
                MqttPublishPacketCodec.Encode(view, _writer);
            }
            else
            {
                using MqttZeroPooledBufferWriter packetWriter = new(
                    _bufferPool,
                    encodedLength);
                MqttPublishPacketCodec.Encode(view, packetWriter);
                _ = await _writer.WriteAsync(
                    packetWriter.WrittenMemory,
                    cancellationToken).ConfigureAwait(false);
            }

            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        MarkPacketSent(encodedLength, true);
    }

    /// <summary>
    /// 获取发布Encoded长度。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="properties">属性。</param>
    /// <param name="payload">负载。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <param name="retain">保留。</param>
    /// <param name="remainingExpiry">对应的 remainingExpiry。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <returns>计算得到的数值。</returns>
    private static int GetPublishEncodedLength(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        ushort packetIdentifier,
        bool duplicate,
        bool retain,
        uint? remainingExpiry,
        ReadOnlyMemory<uint> subscriptionIdentifiers)
    {
        MqttZeroPublishView view = new(
            topic,
            properties,
            payload,
            qualityOfServiceLevel,
            packetIdentifier,
            duplicate,
            retain,
            remainingExpiry,
            null,
            subscriptionIdentifiers,
            null);
        return MqttPublishPacketCodec.GetEncodedLength(view);
    }

    /// <summary>
    /// Mark报文Sent。
    /// </summary>
    /// <param name="byteCount">字节数量。</param>
    /// <param name="applicationMessage">application消息。</param>
    private void MarkPacketSent(int byteCount, bool applicationMessage)
    {
        Interlocked.Increment(ref _packetsSent);
        Interlocked.Add(ref _bytesSent, byteCount);
        if (applicationMessage)
        {
            Interlocked.Increment(ref _messagesSent);
        }
    }

    /// <summary>
    /// Begin控制写入。
    /// </summary>
    private void BeginControlWrite()
    {
        Interlocked.Increment(ref _pendingControlWrites);
    }

    /// <summary>
    /// End控制写入。
    /// </summary>
    private void EndControlWrite()
    {
        Interlocked.Decrement(ref _pendingControlWrites);
    }

    /// <summary>
    /// Enter写入。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<MqttZeroWriteLease> EnterWriteAsync(
        CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (Interlocked.Increment(ref _writersInFlight) != 1)
        {
            Interlocked.Decrement(ref _writersInFlight);
            _writeGate.Release();
            throw new InvalidOperationException(
                "Concurrent writes to one MQTT connection are not allowed.");
        }

        return new MqttZeroWriteLease(this);
    }

    /// <summary>
    /// Acquires the write gate with a bounded wait for writers that run off
    /// the session shard. A gate held for longer than the timeout means the
    /// peer stopped reading, so the connection is aborted instead of letting
    /// the shard wait behind it.
    /// </summary>
    private async ValueTask<MqttZeroWriteLease?> TryEnterWriteAsync(
        CancellationToken cancellationToken)
    {
        bool acquired = await _writeGate
            .WaitAsync(WriteGateTimeout, cancellationToken)
            .ConfigureAwait(false);
        if (!acquired)
        {
            return null;
        }

        if (Interlocked.Increment(ref _writersInFlight) != 1)
        {
            Interlocked.Decrement(ref _writersInFlight);
            _writeGate.Release();
            throw new InvalidOperationException(
                "Concurrent writes to one MQTT connection are not allowed.");
        }

        return new MqttZeroWriteLease(this);
    }

    /// <summary>
    /// 释放写入。
    /// </summary>
    internal void ReleaseWrite()
    {
        Interlocked.Decrement(ref _writersInFlight);
        _writeGate.Release();
    }

    /// <summary>
    /// 获取Remaining消息Expiry。
    /// </summary>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    /// <returns>对应的结果。</returns>
    private uint? GetRemainingMessageExpiry(DateTimeOffset? expiresAt)
    {
        if (!expiresAt.HasValue)
        {
            return null;
        }

        double remainingSeconds =
            Math.Ceiling((expiresAt.Value - _timeProvider.GetUtcNow()).TotalSeconds);
        if (remainingSeconds <= 0)
        {
            return 0;
        }

        return (uint)Math.Min(remainingSeconds, uint.MaxValue);
    }

    /// <summary>
    /// 发送控制报文。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="size">大小。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask SendControlPacketAsync(
        MqttPacket packet,
        MqttZeroControlPacketSize size,
        CancellationToken cancellationToken)
    {
        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(size.EncodedLength);
        if (frame != null)
        {
            MqttPacketCodec.Encode(packet, frame.Writer);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        await SendControlPacketDirectAsync(packet, size, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// 发送控制报文Direct。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="size">大小。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask SendControlPacketDirectAsync(
        MqttPacket packet,
        MqttZeroControlPacketSize size,
        CancellationToken cancellationToken)
    {
        if (size.EncodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            BeginControlWrite();
            try
            {
                MqttPacketCodec.Encode(packet, _writer);
                _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                MarkPacketSent(size.EncodedLength, false);
            }
            finally
            {
                EndControlWrite();
            }
        }
    }

    /// <summary>
    /// 发送确认Direct。
    /// </summary>
    /// <param name="firstByte">第一个字节。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <param name="waitForWrite">wait写入。</param>
    /// <param name="coalesceOnShard">coalesceOn分片。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask SendAckDirectAsync(
        byte firstByte,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken,
        bool waitForWrite = true,
        bool coalesceOnShard = false)
    {
        bool includeReason = reasonCode != MqttReasonCode.Success;
        int encodedLength = includeReason ? 6 : 4;
        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(encodedLength);
        if (frame != null)
        {
            Span<byte> destination = frame.Writer.GetSpan(encodedLength);
            WriteAcknowledgement(
                destination,
                firstByte,
                packetIdentifier,
                includeReason,
                reasonCode);
            frame.Writer.Advance(encodedLength);
            await PostControlFrameAsync(
                frame,
                cancellationToken,
                waitForWrite,
                coalesceOnShard)
                .ConfigureAwait(false);
            return;
        }

        MqttZeroWriteLease? ackLease =
            await TryEnterWriteAsync(cancellationToken).ConfigureAwait(false);
        if (ackLease == null)
        {
            Abort();
            return;
        }

        using (ackLease.Value)
        {
            BeginControlWrite();
            try
            {
                Span<byte> destination = _writer.GetSpan(encodedLength);
                WriteAcknowledgement(
                    destination,
                    firstByte,
                    packetIdentifier,
                    includeReason,
                    reasonCode);
                _writer.Advance(encodedLength);
                _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                MarkPacketSent(encodedLength, false);
            }
            finally
            {
                EndControlWrite();
            }
        }
    }

    /// <summary>
    /// 写入确认。
    /// </summary>
    /// <param name="destination">对应的 destination。</param>
    /// <param name="firstByte">第一个字节。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="includeReason">include原因。</param>
    /// <param name="reasonCode">原因代码。</param>
    private static void WriteAcknowledgement(
        Span<byte> destination,
        byte firstByte,
        ushort packetIdentifier,
        bool includeReason,
        MqttReasonCode reasonCode)
    {
        destination[0] = firstByte;
        destination[1] = includeReason ? (byte)4 : (byte)2;
        BinaryPrimitives.WriteUInt16BigEndian(destination[2..], packetIdentifier);
        if (includeReason)
        {
            destination[4] = (byte)reasonCode;
            destination[5] = 0;
        }
    }

    /// <summary>
    /// 关闭。
    /// </summary>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask CloseAsync(
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        CloseReasonCode = reasonCode;
        DisconnectType = reasonCode switch
        {
            MqttReasonCode.KeepAliveTimeout => MqttZeroDisconnectType.KeepAliveTimeout,
            MqttReasonCode.SessionTakenOver => MqttZeroDisconnectType.SessionTakenOver,
            MqttReasonCode.ServerShuttingDown => MqttZeroDisconnectType.ServerShutdown,
            _ => MqttZeroDisconnectType.ProtocolError
        };
        try
        {
            await SendDisconnectAsync(
                new MqttDisconnectPacket
                {
                    ReasonCode = reasonCode
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
        }
        catch (InvalidOperationException)
        {
        }
        finally
        {
            try
            {
                _closed.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    /// <summary>
    /// 中止。
    /// </summary>
    public void Abort()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        try
        {
            _closed.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        try
        {
            _writer.CancelPendingFlush();
        }
        catch (InvalidOperationException)
        {
        }
    }

    /// <summary>
    /// 尝试租用出站控制帧。
    /// </summary>
    /// <param name="encodedLength">encoded长度。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroEgressControlFrame? TryRentEgressControlFrame(
        int encodedLength)
    {
        MqttZeroSessionEgress? egress = _egress;
        if (egress == null || egress.IsStopping)
        {
            return null;
        }

        return _controlFrames.Rent(encodedLength);
    }

    /// <summary>
    /// 投递控制帧。
    /// </summary>
    /// <param name="frame">帧。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <param name="waitForWrite">wait写入。</param>
    /// <param name="coalesceOnShard">coalesceOn分片。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask PostControlFrameAsync(
        MqttZeroEgressControlFrame frame,
        CancellationToken cancellationToken,
        bool waitForWrite = true,
        bool coalesceOnShard = false)
    {
        bool ownsFrame = true;
        try
        {
            MqttZeroSessionEgress egress = _egress ??
                throw new ObjectDisposedException(nameof(MqttZeroSessionEgress));
            ownsFrame = !await egress.PostControlAsync(
                frame,
                cancellationToken,
                waitForWrite,
                coalesceOnShard).ConfigureAwait(false);
        }
        finally
        {
            if (ownsFrame)
            {
                frame.Release();
            }
        }
    }

    /// <summary>
    /// 释放。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _closed.Cancel();
        _closed.Dispose();
        _controlFrames.Dispose();
        _inboundTopicAliases?.Dispose();
        _inboundTopicAliases = null;
        bool gateAcquired = await TryEnterWriteGateAsync().ConfigureAwait(false);
        try
        {
            await _writer.CompleteAsync().ConfigureAwait(false);
        }
        finally
        {
            if (gateAcquired)
            {
                _writeGate.Release();
                _writeGate.Dispose();
            }
        }
    }

    /// <summary>
    /// 尝试Enter写入Gate。
    /// </summary>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<bool> TryEnterWriteGateAsync()
    {
        try
        {
            return await _writeGate
                .WaitAsync(TimeSpan.FromSeconds(1))
                .ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }
}
