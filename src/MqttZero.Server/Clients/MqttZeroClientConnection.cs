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

    public EndPoint? RemoteEndPoint { get; }

    internal long ConnectionId { get; }

    internal int ShardId { get; set; }

    internal MqttZeroSessionEgress? Egress
    {
        get
        {
            return _egress;
        }
    }

    internal void AttachEgress(MqttZeroSessionEgress egress)
    {
        ArgumentNullException.ThrowIfNull(egress);
        _egress = egress;
    }

    public bool IsSecure { get; }

    public bool IsWebSocket { get; }

    public ushort KeepAliveSeconds { get; }

    public ushort ReceiveMaximum { get; }

    public uint MaximumPacketSize { get; }

    public bool AllowPacketFragmentation { get; }

    public bool RequestProblemInformation { get; }

    public bool RequestResponseInformation { get; }

    public MqttZeroClientConnectionInfo ClientInfo { get; set; } = null!;

    internal bool HasPendingControlWrites
    {
        get
        {
            return Volatile.Read(ref _pendingControlWrites) > 0;
        }
    }

    public bool IsConnectedClientCounted { get; set; }

    public MqttZeroConnectedClient ConnectedClient { get; set; } = null!;

    public MqttZeroDisconnectType DisconnectType { get; set; } = MqttZeroDisconnectType.ConnectionLost;

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

    public bool IsReadingIncompletePacket { get; set; }

    internal bool IsClosed
    {
        get
        {
            return Volatile.Read(ref _disposed) != 0 ||
                _closed.IsCancellationRequested;
        }
    }

    public CancellationToken ClosedToken
    {
        get
        {
            return _closed.Token;
        }
    }

    public void MarkPacketReceived(TimeProvider timeProvider)
    {
        Volatile.Write(
            ref _lastPacketReceivedTicks,
            timeProvider.GetUtcNow().UtcTicks);
        Interlocked.Increment(ref _packetsReceived);
    }

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

    public void SetInboundTopicAlias(ushort alias, ReadOnlySequence<byte> topic)
    {
        (_inboundTopicAliases ??= new MqttZeroTopicAliasTable(
            _bufferPool)).Set(alias, topic);
    }

    public bool TryGetInboundTopicAlias(ushort alias, out ReadOnlyMemory<byte> topic)
    {
        if (_inboundTopicAliases != null)
        {
            return _inboundTopicAliases.TryGet(alias, out topic);
        }

        topic = ReadOnlyMemory<byte>.Empty;
        return false;
    }

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

    internal async ValueTask SendPublishBatchAsync(
        IReadOnlyList<MqttZeroQueuedMessage> messages,
        CancellationToken cancellationToken)
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
                    false,
                    cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        int encodedBytes;
        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            encodedBytes = WritePublishBatch(messages);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        MarkPublishBatchSent(messages.Count, encodedBytes);
    }

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

    private int WritePublishBatch(
        IReadOnlyList<MqttZeroQueuedMessage> messages)
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
                false,
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

    private void MarkPublishBatchSent(
        int messageCount,
        int encodedBytes)
    {
        Interlocked.Add(ref _packetsSent, messageCount);
        Interlocked.Add(ref _bytesSent, encodedBytes);
        Interlocked.Add(ref _messagesSent, messageCount);
    }

    internal async ValueTask<FlushResult> FlushOutputAsync(
        CancellationToken cancellationToken)
    {
        using (await EnterWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            return await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

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

    public ValueTask SendPubRelAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        return SendAckDirectAsync(
            MqttPacketHeader.GetFirstByte(MqttPacketType.PubRel),
            packetIdentifier,
            reasonCode,
            cancellationToken);
    }

    public ValueTask SendPubCompAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        return SendAckDirectAsync(
            MqttPacketHeader.GetFirstByte(MqttPacketType.PubComp),
            packetIdentifier,
            reasonCode,
            cancellationToken);
    }

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

    private void MarkPacketSent(int byteCount, bool applicationMessage)
    {
        Interlocked.Increment(ref _packetsSent);
        Interlocked.Add(ref _bytesSent, byteCount);
        if (applicationMessage)
        {
            Interlocked.Increment(ref _messagesSent);
        }
    }

    private void BeginControlWrite()
    {
        Interlocked.Increment(ref _pendingControlWrites);
    }

    private void EndControlWrite()
    {
        Interlocked.Decrement(ref _pendingControlWrites);
    }

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

    internal void ReleaseWrite()
    {
        Interlocked.Decrement(ref _writersInFlight);
        _writeGate.Release();
    }

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

    private async ValueTask SendAckDirectAsync(
        byte firstByte,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken,
        bool waitForWrite = true)
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
            await PostControlFrameAsync(frame, cancellationToken, waitForWrite)
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

    private async ValueTask PostControlFrameAsync(
        MqttZeroEgressControlFrame frame,
        CancellationToken cancellationToken,
        bool waitForWrite = true)
    {
        bool ownsFrame = true;
        try
        {
            MqttZeroSessionEgress egress = _egress ??
                throw new ObjectDisposedException(nameof(MqttZeroSessionEgress));
            ownsFrame = !await egress.PostControlAsync(
                frame,
                cancellationToken,
                waitForWrite).ConfigureAwait(false);
        }
        finally
        {
            if (ownsFrame)
            {
                frame.Release();
            }
        }
    }

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
