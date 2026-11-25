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
    private readonly MqttZeroPacketWriter _bufferWriter = new();
    private readonly MqttZeroBufferPool _bufferPool;
    private readonly CancellationTokenSource _closed = new();
    private readonly MqttZeroEgressControlFramePool _controlFrames;
    private readonly PipeWriter _writer;
    private readonly DateTimeOffset _connectedAt;
    private MqttZeroEgressActor? _egress;
    private MqttZeroTopicAliasTable? _inboundTopicAliases;
    private readonly TimeProvider _timeProvider;
    private int _disposed;
    private int _pendingControlWrites;
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
        LastPacketReceivedAt = timeProvider.GetUtcNow();
        _connectedAt = LastPacketReceivedAt;
    }

    public EndPoint? RemoteEndPoint { get; }

    internal long ConnectionId { get; }

    internal int ShardId { get; set; }

    internal MqttZeroEgressActor? Egress
    {
        get
        {
            return _egress;
        }
    }

    internal void AttachEgress(MqttZeroEgressActor egress)
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

    public DateTimeOffset LastPacketReceivedAt { get; private set; }

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
        LastPacketReceivedAt = timeProvider.GetUtcNow();
        Interlocked.Increment(ref _packetsReceived);
    }

    public void MarkPacketReceived(
        TimeProvider timeProvider,
        long byteCount,
        bool applicationMessage)
    {
        LastPacketReceivedAt = timeProvider.GetUtcNow();
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

    public void ResetStatistics()
    {
        Interlocked.Exchange(ref _packetsReceived, 0);
        Interlocked.Exchange(ref _packetsSent, 0);
        Interlocked.Exchange(ref _bytesReceived, 0);
        Interlocked.Exchange(ref _bytesSent, 0);
        Interlocked.Exchange(ref _messagesReceived, 0);
        Interlocked.Exchange(ref _messagesSent, 0);
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
        _bufferWriter.Clear();
        MqttPacketCodec.Encode(packet, _bufferWriter);
        if (_bufferWriter.WrittenCount > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(_bufferWriter.WrittenCount);
        if (frame != null)
        {
            Span<byte> destination = frame.Writer.GetSpan(
                _bufferWriter.WrittenCount);
            _bufferWriter.WrittenMemory.Span.CopyTo(destination);
            frame.Writer.Advance(_bufferWriter.WrittenCount);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        BeginControlWrite();
        try
        {
            _ = await _writer.WriteAsync(_bufferWriter.WrittenMemory, cancellationToken).ConfigureAwait(false);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            MarkPacketSent(
                _bufferWriter.WrittenCount,
                packet is MqttPublishPacket);
        }
        finally
        {
            EndControlWrite();
        }
    }

    internal async ValueTask WriteEncodedControlBatchAsync(
        IReadOnlyList<MqttZeroEgressControlFrame> frames,
        CancellationToken cancellationToken)
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
            int encodedLength = message.EncodedLength > 0 &&
                message.ExpiresAt == null
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

        _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        Interlocked.Add(ref _packetsSent, messages.Count);
        Interlocked.Add(ref _bytesSent, encodedBytes);
        Interlocked.Add(ref _messagesSent, messages.Count);
    }

    internal bool TryEncodePublishBatch(
        IReadOnlyList<MqttZeroQueuedMessage> messages,
        out int encodedBytes)
    {
        encodedBytes = 0;
        if (!AllowPacketFragmentation || messages.Count == 0)
        {
            return false;
        }

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
            int encodedLength = message.EncodedLength > 0 &&
                message.ExpiresAt == null
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

        Interlocked.Add(ref _packetsSent, messages.Count);
        Interlocked.Add(ref _bytesSent, encodedBytes);
        Interlocked.Add(ref _messagesSent, messages.Count);
        return true;
    }

    internal ValueTask<FlushResult> FlushOutputAsync(
        CancellationToken cancellationToken)
    {
        return _writer.FlushAsync(cancellationToken);
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
        await SendPublishAsyncWithIdentifiers(
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

    public async ValueTask SendPublishAsyncWithIdentifiers(
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

    internal bool IsPublishTooLarge(
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
        MqttZeroPublishView view = new(
            lease.TopicSequence,
            lease.PropertiesSequence,
            lease.PayloadSequence,
            qualityOfServiceLevel,
            0,
            false,
            retain,
            GetRemainingMessageExpiry(lease.ExpiresAt),
            null,
            subscriptionIdentifiers,
            null);
        return MqttPublishPacketCodec.GetEncodedLength(view);
    }

    public async ValueTask SendPubAckAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        int encodedLength = reasonCode == MqttReasonCode.Success ? 4 : 6;
        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(encodedLength);
        if (frame != null)
        {
            bool includeReason = reasonCode != MqttReasonCode.Success;
            Span<byte> destination = frame.Writer.GetSpan(encodedLength);
            destination[0] = 0x40;
            destination[1] = includeReason ? (byte)4 : (byte)2;
            BinaryPrimitives.WriteUInt16BigEndian(
                destination[2..],
                packetIdentifier);
            if (includeReason)
            {
                destination[4] = (byte)reasonCode;
                destination[5] = 0;
            }

            frame.Writer.Advance(encodedLength);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        BeginControlWrite();
        try
        {
            bool includeReason = reasonCode != MqttReasonCode.Success;
            Span<byte> header = _writer.GetSpan(includeReason ? 6 : 4);
            header[0] = 0x40;
            header[1] = includeReason ? (byte)4 : (byte)2;
            BinaryPrimitives.WriteUInt16BigEndian(header[2..], packetIdentifier);
            _writer.Advance(4);
            if (includeReason)
            {
                _writer.GetSpan(1)[0] = (byte)reasonCode;
                _writer.Advance(1);
                _writer.GetSpan(1)[0] = 0;
                _writer.Advance(1);
            }

            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            MarkPacketSent(encodedLength, false);
        }
        finally
        {
            EndControlWrite();
        }
    }

    public ValueTask SendPubRecAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        return SendAckDirectAsync(
            0x50,
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
            0x62,
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
            0x70,
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
            destination[0] = 0xD0;
            destination[1] = 0;
            frame.Writer.Advance(2);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        BeginControlWrite();
        try
        {
            Span<byte> header = _writer.GetSpan(2);
            header[0] = 0xD0;
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

    public async ValueTask SendSubAckAsync(
        MqttSubAckPacket packet,
        CancellationToken cancellationToken)
    {
        int propertiesLength = Mqtt5PropertyWriter.GetReasonPropertiesLength(
            packet.ReasonString,
            packet.UserProperties);
        int bodyLength =
            2 +
            GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength +
            packet.ReasonCodes.Length;
        long encodedLength =
            1 +
            GetVariableByteIntegerSize(bodyLength) +
            bodyLength;
        if (encodedLength > MaximumPacketSize &&
            (packet.ReasonString != null || packet.UserProperties.Count > 0))
        {
            packet.ReasonString = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
            propertiesLength = 0;
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(checked((int)encodedLength));
        if (frame != null)
        {
            MqttPacketCodec.Encode(packet, frame.Writer);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        await SendControlPacketDirectAsync(
            packet,
            2 + packet.ReasonCodes.Length,
            propertiesLength,
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SendUnsubAckAsync(
        MqttUnsubAckPacket packet,
        CancellationToken cancellationToken)
    {
        int propertiesLength = Mqtt5PropertyWriter.GetReasonPropertiesLength(
            packet.ReasonString,
            packet.UserProperties);
        int bodyLength =
            2 +
            GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength +
            packet.ReasonCodes.Length;
        long encodedLength =
            1 +
            GetVariableByteIntegerSize(bodyLength) +
            bodyLength;
        if (encodedLength > MaximumPacketSize &&
            (packet.ReasonString != null || packet.UserProperties.Count > 0))
        {
            packet.ReasonString = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
            propertiesLength = 0;
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(checked((int)encodedLength));
        if (frame != null)
        {
            MqttPacketCodec.Encode(packet, frame.Writer);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        await SendControlPacketDirectAsync(
            packet,
            2 + packet.ReasonCodes.Length,
            propertiesLength,
            cancellationToken).ConfigureAwait(false);
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

        int propertiesLength = Mqtt5PropertyWriter.GetConnAckPropertiesLength(packet);
        int bodyLength =
            2 +
            GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength;
        long encodedLength =
            1 +
            GetVariableByteIntegerSize(bodyLength) +
            bodyLength;
        if (encodedLength > MaximumPacketSize &&
            (packet.ReasonString != null || packet.UserProperties.Count > 0))
        {
            packet.ReasonString = null;
            packet.UserProperties = Array.Empty<MqttUserProperty>();
            propertiesLength = Mqtt5PropertyWriter.GetConnAckPropertiesLength(packet);
            bodyLength =
                2 +
                GetVariableByteIntegerSize(propertiesLength) +
                propertiesLength;
            encodedLength =
                1 +
                GetVariableByteIntegerSize(bodyLength) +
                bodyLength;
        }

        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(checked((int)encodedLength));
        if (frame != null)
        {
            MqttPacketCodec.Encode(packet, frame.Writer);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        BeginControlWrite();
        try
        {
            Span<byte> fixedHeader = _writer.GetSpan(5);
            fixedHeader[0] = 0x20;
            int lengthBytes = MqttZeroPacketWriter.WriteVariableByteInteger(
                fixedHeader[1..],
                (uint)bodyLength);
            _writer.Advance(1 + lengthBytes);
            Span<byte> acknowledgeHeader = _writer.GetSpan(2);
            acknowledgeHeader[0] = packet.SessionPresent ? (byte)1 : (byte)0;
            acknowledgeHeader[1] = (byte)packet.ReasonCode;
            _writer.Advance(2);
            Mqtt5PropertyWriter.WriteConnAckProperties(packet, _writer);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            MarkPacketSent((int)encodedLength, false);
        }
        finally
        {
            EndControlWrite();
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
        int bodyLength = includeDetail
            ? 1 +
                GetVariableByteIntegerSize(propertiesLength) +
                propertiesLength
            : 0;
        long encodedLength =
            1 +
            GetVariableByteIntegerSize(bodyLength) +
            bodyLength;
        if (encodedLength > MaximumPacketSize &&
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
            bodyLength = includeDetail
                ? 1 +
                    GetVariableByteIntegerSize(propertiesLength) +
                    propertiesLength
                : 0;
            encodedLength =
                1 +
                GetVariableByteIntegerSize(bodyLength) +
                bodyLength;
        }

        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        MqttZeroEgressControlFrame? frame =
            TryRentEgressControlFrame(checked((int)encodedLength));
        if (frame != null)
        {
            MqttPacketCodec.Encode(packet, frame.Writer);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        BeginControlWrite();
        try
        {
            Span<byte> fixedHeader = _writer.GetSpan(5);
            fixedHeader[0] = 0xE0;
            int lengthBytes = MqttZeroPacketWriter.WriteVariableByteInteger(
                fixedHeader[1..],
                (uint)bodyLength);
            _writer.Advance(1 + lengthBytes);
            if (includeDetail)
            {
                _writer.GetSpan(1)[0] = (byte)packet.ReasonCode;
                _writer.Advance(1);
                Mqtt5PropertyWriter.WriteDisconnectProperties(packet, _writer);
            }

            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            MarkPacketSent((int)encodedLength, false);
        }
        finally
        {
            EndControlWrite();
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
        MqttZeroPublishView view = new(
            topic,
            properties,
            payload,
            qualityOfServiceLevel,
            packetIdentifier,
            duplicate,
            retain,
            GetRemainingMessageExpiry(expiresAt),
            null,
            subscriptionIdentifiers,
            null);
        int encodedLength = MqttPublishPacketCodec.GetEncodedLength(view);
        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

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
        MarkPacketSent(encodedLength, true);
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

    private async ValueTask SendControlPacketDirectAsync(
        MqttPacket packet,
        long bodyLength,
        long propertyLength,
        CancellationToken cancellationToken)
    {
        int propertyLengthSize = propertyLength < 128
            ? 1
            : propertyLength < 16_384
                ? 2
                : propertyLength < 2_097_152
                    ? 3
                    : 4;
        bodyLength += propertyLength + propertyLengthSize;
        int remainingLengthSize = GetVariableByteIntegerSize(bodyLength);
        long encodedLength = 1 + remainingLengthSize + bodyLength;
        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        BeginControlWrite();
        try
        {
            MqttPacketCodec.Encode(packet, _writer);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            MarkPacketSent((int)encodedLength, false);
        }
        finally
        {
            EndControlWrite();
        }
    }

    private async ValueTask SendAckDirectAsync(
        byte firstByte,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
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
            destination[0] = firstByte;
            destination[1] = includeReason ? (byte)4 : (byte)2;
            BinaryPrimitives.WriteUInt16BigEndian(
                destination[2..],
                packetIdentifier);
            if (includeReason)
            {
                destination[4] = (byte)reasonCode;
                destination[5] = 0;
            }

            frame.Writer.Advance(encodedLength);
            await PostControlFrameAsync(frame, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        BeginControlWrite();
        try
        {
            Span<byte> destination = _writer.GetSpan(encodedLength);
            destination[0] = firstByte;
            destination[1] = includeReason ? (byte)4 : (byte)2;
            BinaryPrimitives.WriteUInt16BigEndian(
                destination[2..],
                packetIdentifier);
            if (includeReason)
            {
                destination[4] = (byte)reasonCode;
                destination[5] = 0;
            }

            _writer.Advance(encodedLength);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            MarkPacketSent(encodedLength, false);
        }
        finally
        {
            EndControlWrite();
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

    private static int GetVariableByteIntegerSize(long value)
    {
        if (value < 128)
        {
            return 1;
        }

        if (value < 16_384)
        {
            return 2;
        }

        if (value < 2_097_152)
        {
            return 3;
        }

        return 4;
    }

    private MqttZeroEgressControlFrame? TryRentEgressControlFrame(
        int encodedLength)
    {
        MqttZeroEgressActor? egress = _egress;
        if (egress == null || egress.IsStopping)
        {
            return null;
        }

        return _controlFrames.Rent(encodedLength);
    }

    private async ValueTask PostControlFrameAsync(
        MqttZeroEgressControlFrame frame,
        CancellationToken cancellationToken)
    {
        bool ownsFrame = true;
        try
        {
            MqttZeroEgressActor egress = _egress ??
                throw new ObjectDisposedException(nameof(MqttZeroEgressActor));
            ownsFrame = !await egress.PostControlAsync(
                frame,
                cancellationToken).ConfigureAwait(false);
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
        await _writer.CompleteAsync().ConfigureAwait(false);
    }
}
