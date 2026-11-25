using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Security.Cryptography.X509Certificates;
using MqttZero.Server.Observers;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements connection acceptance, CONNECT processing, and takeover.
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// Processes one accepted Kestrel connection until it disconnects.
    /// </summary>
    /// <param name="input">The inbound byte pipe.</param>
    /// <param name="output">The outbound byte pipe.</param>
    /// <param name="remoteEndPoint">The optional remote network endpoint.</param>
    /// <param name="isSecure">Whether the connection uses TLS.</param>
    /// <param name="isWebSocket">Whether the connection uses WebSocket.</param>
    /// <param name="cancellationToken">A token that terminates the connection.</param>
    /// <returns>A task that completes when the connection closes.</returns>
    public ValueTask AcceptConnectionAsync(
        PipeReader input,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        CancellationToken cancellationToken)
    {
        return AcceptConnectionAsync(
            new MqttZeroTransportConnection(
                input,
                output,
                remoteEndPoint,
                isSecure,
                isWebSocket,
                allowPacketFragmentation: true),
            clientCertificate: null,
            cancellationToken);
    }

    /// <summary>
    /// Processes one accepted Kestrel connection until it disconnects.
    /// </summary>
    /// <param name="input">The inbound byte pipe.</param>
    /// <param name="output">The outbound byte pipe.</param>
    /// <param name="remoteEndPoint">The optional remote network endpoint.</param>
    /// <param name="isSecure">Whether the connection uses TLS.</param>
    /// <param name="isWebSocket">Whether the connection uses WebSocket.</param>
    /// <param name="allowPacketFragmentation">Whether outbound PUBLISH packets may be split across writes.</param>
    /// <param name="cancellationToken">A token that terminates the connection.</param>
    /// <returns>A task that completes when the connection closes.</returns>
    public ValueTask AcceptConnectionAsync(
        PipeReader input,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        bool allowPacketFragmentation,
        CancellationToken cancellationToken)
    {
        return AcceptConnectionAsync(
            new MqttZeroTransportConnection(
                input,
                output,
                remoteEndPoint,
                isSecure,
                isWebSocket,
                allowPacketFragmentation),
            clientCertificate: null,
            cancellationToken);
    }

    /// <summary>Processes one accepted connection with client certificate data.</summary>
    public ValueTask AcceptConnectionAsync(
        PipeReader input,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        X509Certificate2? clientCertificate,
        bool allowPacketFragmentation,
        CancellationToken cancellationToken)
    {
        return AcceptConnectionAsync(
            new MqttZeroTransportConnection(
                input,
                output,
                remoteEndPoint,
                isSecure,
                isWebSocket,
                allowPacketFragmentation),
            clientCertificate,
            cancellationToken);
    }

    internal ValueTask AcceptConnectionAsync(
        MqttZeroTransportConnection transport,
        X509Certificate2? clientCertificate,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transport);
        return AcceptConnectionCoreAsync(
            transport.Input,
            transport.Output,
            transport.RemoteEndPoint,
            transport.IsSecure,
            transport.IsWebSocket,
            clientCertificate,
            transport.AllowPacketFragmentation,
            cancellationToken);
    }

    private async ValueTask AcceptConnectionCoreAsync(
        PipeReader input,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        X509Certificate2? clientCertificate,
        bool allowPacketFragmentation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);

        long acceptedConnectionId = Interlocked.Increment(ref _nextAcceptedConnectionId);
        using CancellationTokenSource acceptedCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bool accepted = false;
        lock (_sessionGate)
        {
            if (_options.AcceptNewConnections &&
                State == MqttZeroBrokerState.Running &&
                Volatile.Read(ref _activeConnectionCount) <
                    _options.MaxActiveConnections)
            {
                Interlocked.Increment(ref _activeConnectionCount);
                _acceptedConnections[acceptedConnectionId] = acceptedCancellation;
                accepted = true;
            }
        }

        if (!accepted)
        {
            await output.CompleteAsync().ConfigureAwait(false);
            return;
        }

        MqttZeroClientConnection? connection = null;
        MqttZeroSessionState? session = null;
        CancellationTokenRegistration closeRegistration = default;
        bool connectionDisposed = false;
        bool cleanDisconnect = false;
        DateTimeOffset? incompleteSince = null;
        try
        {
            using CancellationTokenSource connectionLifetime =
                CancellationTokenSource.CreateLinkedTokenSource(acceptedCancellation.Token);
            using CancellationTokenSource connectTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(connectionLifetime.Token);
            connectTimeout.CancelAfter(_options.ConnectTimeout);

            while (!connectionLifetime.IsCancellationRequested)
            {
                CancellationToken readToken = session == null
                    ? connectTimeout.Token
                    : connectionLifetime.Token;
                using CancellationTokenSource? incompleteTimeout =
                    session != null && incompleteSince.HasValue
                        ? CancellationTokenSource.CreateLinkedTokenSource(readToken)
                        : null;
                if (incompleteTimeout != null)
                {
                    TimeSpan remaining =
                        _options.IncompletePacketTimeout -
                        (_timeProvider.GetUtcNow() - incompleteSince!.Value);
                    if (remaining <= TimeSpan.Zero)
                    {
                        throw new OperationCanceledException(
                            "The incomplete MQTT packet timed out.");
                    }

                    incompleteTimeout.CancelAfter(remaining);
                    readToken = incompleteTimeout.Token;
                }

                ReadResult readResult = await input.ReadAsync(readToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = readResult.Buffer;
                SequencePosition examined = readResult.Buffer.End;

                if (MqttPacketCodec.TryGetPacketLength(buffer, out long packetLength, out _) &&
                    packetLength > _options.MaxPacketSize)
                {
                    throw new InvalidOperationException("The inbound MQTT packet exceeds the broker maximum.");
                }

                while (true)
                {
                    if (session != null &&
                        !buffer.IsEmpty &&
                        (buffer.Slice(0, 1).FirstSpan[0] >> 4) == 0x0F)
                    {
                        await connection!.CloseAsync(
                            MqttReasonCode.ProtocolError,
                            connectionLifetime.Token).ConfigureAwait(false);
                        return;
                    }

                    if (session != null &&
                        MqttPublishPacketCodec.TryDecode(
                            buffer,
                            out MqttZeroPublishView publishView,
                            out long publishConsumed))
                    {
                        connection!.MarkPacketReceived(
                            _timeProvider,
                            publishConsumed,
                            true);
                        try
                        {
                            MqttZeroMessageLease lease;
                            if (!TryCreateMessageLease(
                                    connection,
                                    publishView,
                                    out MqttZeroMessageLease? candidate,
                                    out MqttReasonCode reasonCode))
                            {
                                if (publishView.QualityOfServiceLevel ==
                                        MqttQualityOfServiceLevel.AtLeastOnce &&
                                    reasonCode == MqttReasonCode.PayloadFormatInvalid)
                                {
                                    await connection.SendPubAckAsync(
                                        publishView.PacketIdentifier,
                                        reasonCode,
                                        connectionLifetime.Token).ConfigureAwait(false);
                                }
                                else if (publishView.QualityOfServiceLevel ==
                                        MqttQualityOfServiceLevel.ExactlyOnce &&
                                    reasonCode == MqttReasonCode.PayloadFormatInvalid)
                                {
                                    await connection.SendPubRecAsync(
                                        publishView.PacketIdentifier,
                                        reasonCode,
                                        connectionLifetime.Token).ConfigureAwait(false);
                                }
                                else
                                {
                                    await connection.CloseAsync(
                                        reasonCode,
                                        connectionLifetime.Token).ConfigureAwait(false);
                                    return;
                                }

                                buffer = buffer.Slice(publishConsumed);
                                continue;
                            }

                            lease = candidate!;
                            bool transferred = false;
                            try
                            {
                                if (lease.QualityOfServiceLevel ==
                                    MqttQualityOfServiceLevel.ExactlyOnce)
                                {
                                    transferred = await AcceptInboundQos2Async(
                                        session,
                                        connection,
                                        lease,
                                        publishView.PacketIdentifier,
                                        connectionLifetime.Token).ConfigureAwait(false);
                                }
                                else if (lease.QualityOfServiceLevel ==
                                        MqttQualityOfServiceLevel.AtLeastOnce &&
                                    CanDispatchInbound)
                                {
                                    MqttZeroPreparedPublish prepared =
                                        await PreparePublishForDispatchAsync(
                                            session,
                                            connection,
                                            lease,
                                            connectionLifetime.Token).ConfigureAwait(false);
                                    if (prepared.Result ==
                                        MqttZeroInterceptionResult.CloseConnection)
                                    {
                                        await connection.CloseAsync(
                                            prepared.ReasonCode == MqttReasonCode.Success
                                                ? MqttReasonCode.NotAuthorized
                                                : prepared.ReasonCode,
                                            connectionLifetime.Token).ConfigureAwait(false);
                                        return;
                                    }

                                    if (prepared.Result !=
                                            MqttZeroInterceptionResult.Continue ||
                                        prepared.Lease == null)
                                    {
                                        await connection.SendPubAckAsync(
                                            lease.PacketIdentifier,
                                            prepared.ReasonCode,
                                            connectionLifetime.Token).ConfigureAwait(false);
                                    }
                                    else
                                    {
                                        long messageId = _messageStore.IsEnabled
                                            ? GetNextMessageId()
                                            : 0;
                                        try
                                        {
                                            await SaveInboundDispatchAsync(
                                                prepared.Lease,
                                                session,
                                                messageId,
                                                connectionLifetime.Token).ConfigureAwait(false);
                                        }
                                        catch
                                        {
                                            if (!prepared.UsesSourceLease)
                                            {
                                                prepared.Lease.Release();
                                            }

                                            throw;
                                        }
                                        try
                                        {
                                            await EnqueueInboundAsync(
                                                prepared.Lease,
                                                session,
                                                connection,
                                                connectionLifetime.Token,
                                                messageId).ConfigureAwait(false);
                                        }
                                        catch
                                        {
                                            await DeleteInboundDispatchAsync(
                                                session.ClientId,
                                                messageId,
                                                CancellationToken.None).ConfigureAwait(false);
                                            if (!prepared.UsesSourceLease)
                                            {
                                                prepared.Lease.Release();
                                            }

                                            throw;
                                        }

                                        transferred = true;
                                        if (!prepared.UsesSourceLease)
                                        {
                                            lease.Release();
                                        }

                                        prepared.Lease.Release();
                                        await connection.SendPubAckAsync(
                                            lease.PacketIdentifier,
                                            MqttReasonCode.Success,
                                            connectionLifetime.Token).ConfigureAwait(false);
                                    }
                                }
                                else
                                {
                                    await RoutePublishLeaseAsync(
                                        session,
                                        connection,
                                        lease,
                                        connectionLifetime.Token).ConfigureAwait(false);
                                }
                            }
                            finally
                            {
                                if (!transferred)
                                {
                                    lease.Release();
                                }
                            }
                        }
                        catch (MqttZeroBufferPoolExhaustedException)
                        {
                            await _bufferPool.WaitForCapacityAsync(connectionLifetime.Token)
                                .ConfigureAwait(false);
                            continue;
                        }

                        buffer = buffer.Slice(publishConsumed);
                        continue;
                    }

                    if (session != null && !buffer.IsEmpty)
                    {
                        MqttPacketType inboundPacketType =
                            (MqttPacketType)(buffer.Slice(0, 1).FirstSpan[0] >> 4);
                        if (MqttPacketCodec.IsAcknowledgementPacketType(
                                inboundPacketType))
                        {
                            if (MqttPacketCodec.TryDecodeAcknowledgement(
                                    buffer,
                                    out MqttZeroAcknowledgementView acknowledgement,
                                    out long acknowledgementConsumed))
                            {
                                connection!.MarkPacketReceived(
                                    _timeProvider,
                                    acknowledgementConsumed,
                                    false);
                                switch (acknowledgement.PacketType)
                                {
                                    case MqttPacketType.PubAck:
                                        await ProcessOutboundPubAckAsync(
                                            session,
                                            acknowledgement.PacketIdentifier,
                                            connectionLifetime.Token).ConfigureAwait(false);
                                        break;
                                    case MqttPacketType.PubRec:
                                        await ProcessOutboundPubRecAsync(
                                            session,
                                            connection!,
                                            acknowledgement.PacketIdentifier,
                                            acknowledgement.ReasonCode,
                                            connectionLifetime.Token).ConfigureAwait(false);
                                        break;
                                    case MqttPacketType.PubRel:
                                        await ProcessInboundPubRelAsync(
                                            session,
                                            connection!,
                                            acknowledgement.PacketIdentifier,
                                            connectionLifetime.Token).ConfigureAwait(false);
                                        break;
                                    case MqttPacketType.PubComp:
                                        await ProcessOutboundPubCompAsync(
                                            session,
                                            acknowledgement.PacketIdentifier,
                                            connectionLifetime.Token).ConfigureAwait(false);
                                        break;
                                }

                                buffer = buffer.Slice(acknowledgementConsumed);
                                continue;
                            }

                            if (MqttPacketCodec.TryGetPacketLength(
                                    buffer,
                                    out _,
                                    out bool acknowledgementComplete) &&
                                acknowledgementComplete)
                            {
                                await connection!.CloseAsync(
                                    MqttReasonCode.ProtocolError,
                                    connectionLifetime.Token).ConfigureAwait(false);
                                return;
                            }

                            break;
                        }
                    }

                    if (!MqttPacketCodec.TryDecode(
                        buffer,
                        out MqttPacket? packet,
                        out long consumed) ||
                        packet == null)
                    {
                        break;
                    }

                    if (session == null)
                    {
                        if (packet is not MqttConnectPacket connectPacket)
                        {
                            return;
                        }

                        bool assignedClientIdentifier =
                            connectPacket.ClientId.IsEmpty;
                        string connectClientId =
                            assignedClientIdentifier
                                ? $"mqttzero-{Guid.NewGuid():N}"
                                : Encoding.UTF8.GetString(
                                    connectPacket.ClientId.ToArray());
                        int sessionShardId =
                            _sessionShardMap!.GetShardId(
                                connectClientId);
                        ConnectionAttachment attachment = await
                            _shardSet![sessionShardId]
                                .InvokeAsync(
                                    token => ConnectAsync(
                                        connectPacket,
                                        output,
                                        remoteEndPoint,
                                        isSecure,
                                        isWebSocket,
                                        clientCertificate,
                                        allowPacketFragmentation,
                                        connectClientId,
                                        assignedClientIdentifier,
                                        sessionShardId,
                                        token),
                                    connectionLifetime.Token)
                                .ConfigureAwait(false);
                        if (attachment == null)
                        {
                            return;
                        }
                        session = attachment.Session;
                        connection = attachment.Connection;
                        closeRegistration = connection.ClosedToken.Register(
                            static state => ((CancellationTokenSource)state!).Cancel(),
                            connectionLifetime);
                        StartEgress(
                            session,
                            connection,
                            connectionLifetime.Token);
                        buffer = buffer.Slice(consumed);
                        continue;
                    }

                    MqttZeroClientConnection currentConnection = connection!;
                    MqttZeroSessionState currentSession = session!;
                    await _shardSet![currentConnection.ShardId]
                        .InvokeAsync(
                            async token =>
                            {
                                currentConnection.MarkPacketReceived(
                                    _timeProvider,
                                    consumed,
                                    packet is MqttPublishPacket);
                                await ProcessPacketAsync(
                                    currentSession,
                                    currentConnection,
                                    packet,
                                    token).ConfigureAwait(false);
                                return true;
                            },
                            connectionLifetime.Token)
                        .ConfigureAwait(false);
                    buffer = buffer.Slice(consumed);

                    if (packet is MqttDisconnectPacket)
                    {
                        cleanDisconnect = true;
                        currentConnection.DisconnectType = MqttZeroDisconnectType.ClientRequest;
                        return;
                    }
                }

                bool incompletePacket = !buffer.IsEmpty;
                if (connection != null)
                {
                    connection.IsReadingIncompletePacket = incompletePacket;
                }

                if (!incompletePacket)
                {
                    incompleteSince = null;
                }
                else
                {
                    incompleteSince ??= _timeProvider.GetUtcNow();
                }

                if (readResult.IsCompleted)
                {
                    input.AdvanceTo(buffer.Start);
                    break;
                }

                input.AdvanceTo(buffer.Start, examined);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (InvalidOperationException) when (session == null)
        {
        }
        finally
        {
            closeRegistration.Dispose();
            try
            {
                try
                {
                    if (connection != null)
                    {
                        ReleaseConnectedClientCount(connection);
                    }

                    if (session != null && connection != null)
                    {
                        connection.IsReadingIncompletePacket = false;
                        await DetachConnectionAsync(session, connection, cleanDisconnect).ConfigureAwait(false);
                        MqttZeroClientDisconnectedContext disconnectedContext = new()
                        {
                            Client = connection.ClientInfo,
                            DisconnectType = connection.DisconnectType,
                            ReasonCode = connection.CloseReasonCode
                        };
                        foreach (IMqttZeroConnectionObserver observer in _connectionObservers)
                        {
                            try
                            {
                                await observer.OnDisconnectedAsync(
                                    disconnectedContext,
                                    CancellationToken.None).ConfigureAwait(false);
                            }
                            catch (Exception exception)
                            {
                                MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
                            }
                        }
                    }
                }
                finally
                {
                    if (connection != null && !connectionDisposed)
                    {
                        connectionDisposed = true;
                        await connection.DisposeAsync().ConfigureAwait(false);
                    }
                }
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogConnectionCleanupFailure(_logger, exception);
            }
            finally
            {
                _acceptedConnections.TryRemove(acceptedConnectionId, out _);
                int remaining = Interlocked.Decrement(ref _activeConnectionCount);
                if (remaining == 0)
                {
                    _connectionsDrained.TrySetResult();
                }

                try
                {
                    await input.CompleteAsync().ConfigureAwait(false);
                }
                finally
                {
                    if (connection == null)
                    {
                        await output.CompleteAsync().ConfigureAwait(false);
                    }
                }
            }
        }
    }

    private async ValueTask<ConnectionAttachment> ConnectAsync(
        MqttConnectPacket connectPacket,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        X509Certificate2? clientCertificate,
        bool allowPacketFragmentation,
        string clientId,
        bool assignedClientIdentifier,
        int sessionShardId,
        CancellationToken cancellationToken)
    {
        uint requestedExpiry = connectPacket.SessionExpiryInterval ?? 0;
        uint effectiveExpiry = ClampSessionExpiry(requestedExpiry);
        DateTimeOffset now = _timeProvider.GetUtcNow();
        MqttZeroClientConnection newConnection = new(
            output,
            remoteEndPoint,
            isSecure,
            isWebSocket,
            connectPacket.KeepAliveSeconds,
            connectPacket.ReceiveMaximum,
            connectPacket.MaximumPacketSize,
            _timeProvider,
            _bufferPool,
            allowPacketFragmentation,
            connectPacket.RequestProblemInformation,
            connectPacket.RequestResponseInformation,
            sessionShardId);
        MqttZeroClientConnectionInfo clientInfo = new()
        {
            ClientId = clientId,
            UserName = connectPacket.HasUserName
                ? Encoding.UTF8.GetString(connectPacket.UserName.ToArray())
                : null,
            RemoteEndPoint = remoteEndPoint,
            IsSecure = isSecure,
            IsWebSocket = isWebSocket,
            ProtocolVersion = connectPacket.ProtocolVersion,
            ClientCertificate = clientCertificate
        };
        newConnection.ClientInfo = clientInfo;
        if (connectPacket.AuthenticationMethod != null)
        {
            try
            {
                await newConnection.SendConnAckAsync(
                    new MqttConnAckPacket
                    {
                        ReasonCode = MqttReasonCode.BadAuthenticationMethod
                    },
                    CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                await newConnection.DisposeAsync().ConfigureAwait(false);
            }

            return null!;
        }

        if (connectPacket.HasWill &&
            connectPacket.WillProperties.PayloadFormatIndicator ==
                MqttPayloadFormatIndicator.Utf8 &&
            !MqttZeroProtocolValidator.IsValidUtf8(connectPacket.WillPayload))
        {
            try
            {
                await newConnection.SendConnAckAsync(
                    new MqttConnAckPacket
                    {
                        ReasonCode = MqttReasonCode.PayloadFormatInvalid
                    },
                    CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                await newConnection.DisposeAsync().ConfigureAwait(false);
            }

            return null!;
        }

        MqttZeroConnectionContext connectionContext = new()
        {
            Client = clientInfo,
            ConnectPacket = connectPacket
        };
        foreach (IMqttZeroConnectionInterceptor interceptor in _connectionInterceptors)
        {
            MqttZeroConnectionInterceptionResult result;
            try
            {
                result = await interceptor.InterceptAsync(connectionContext, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
                result = MqttZeroConnectionInterceptionResult.Reject;
                connectionContext.RejectReasonCode = MqttReasonCode.ImplementationSpecificError;
            }

            if (result == MqttZeroConnectionInterceptionResult.Drop)
            {
                await newConnection.DisposeAsync().ConfigureAwait(false);
                return null!;
            }

            if (result == MqttZeroConnectionInterceptionResult.Reject)
            {
                try
                {
                    await newConnection.SendConnAckAsync(
                        new MqttConnAckPacket
                        {
                            ReasonCode = connectionContext.RejectReasonCode,
                            ReasonString = connectionContext.ReasonString
                        },
                        CancellationToken.None).ConfigureAwait(false);
                }
                finally
                {
                    await newConnection.DisposeAsync().ConfigureAwait(false);
                }

                return null!;
            }
        }

        SessionMutationLease mutation;
        try
        {
            mutation = await AcquireSessionMutationAsync(
                clientId,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await newConnection.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        using (mutation)
        {
            MqttZeroClientConnection? oldConnection = null;
            MqttZeroWillMessage? takeoverWill = null;
            MqttZeroSessionState session;
            bool sessionPresent;
            bool replacingSession = false;
            bool newSessionPersisted = false;
            bool existingPersistenceDeleted = false;
            MqttZeroSessionState? replacedSession = null;
            bool previousCleanStart = false;
            uint previousExpiryInterval = 0;
            DateTimeOffset? previousDisconnectedAt = null;
            MqttZeroWillMessage? previousWillMessage = null;
            DateTimeOffset? previousWillDueAt = null;
            MqttZeroSessionState? previousPumpSession = null;
            MqttZeroEgressActor? previousPump = null;
            bool previousPumpStopped = false;

            try
            {
                lock (_sessionGate)
                {
                    _sessions.TryGetValue(clientId, out MqttZeroSessionState? existing);
                    int expectedShardId =
                        _sessionShardMap!.GetShardId(clientId);
                    if (existing != null &&
                        existing.ShardId != expectedShardId)
                    {
                        throw new InvalidOperationException(
                            "The MQTT session is attached to an unexpected event loop shard.");
                    }

                    if (existing != null &&
                        (connectPacket.CleanStart || IsSessionExpired(existing, now)))
                    {
                        replacedSession = existing;
                        replacingSession = true;
                        lock (existing.SyncRoot)
                        {
                            oldConnection = existing.Connection;
                            previousPumpSession = existing;
                            previousPump = existing.Egress;
                            existing.Egress = null;
                            if (oldConnection != null &&
                                existing.WillMessage != null &&
                                existing.WillMessage.DelayInterval == 0)
                            {
                                takeoverWill = existing.WillMessage;
                            }
                        }

                        session = CreateSessionState(clientId);
                        sessionPresent = false;
                    }
                    else if (existing == null)
                    {
                        if (_sessions.Count >= _options.MaxSessions)
                        {
                            throw new InvalidOperationException("The maximum number of MQTT sessions was reached.");
                        }

                        session = CreateSessionState(clientId);
                        sessionPresent = false;
                    }
                    else
                    {
                        session = existing;
                        sessionPresent = true;
                        lock (session.SyncRoot)
                        {
                            previousCleanStart = session.CleanStart;
                            previousExpiryInterval = session.ExpiryInterval;
                            previousDisconnectedAt = session.DisconnectedAt;
                            previousWillMessage = session.WillMessage;
                            previousWillDueAt = session.WillDueAt;
                            previousPumpSession = session;
                            previousPump = session.Egress;
                            session.Egress = null;
                        }
                    }

                    lock (session.SyncRoot)
                    {
                        MqttZeroClientConnection? currentConnection = session.Connection;
                        if (currentConnection != null)
                        {
                            oldConnection = currentConnection;
                            if (session.WillMessage != null &&
                                session.WillMessage.DelayInterval == 0)
                            {
                                takeoverWill = session.WillMessage;
                            }
                        }

                        session.Connection = newConnection;
                        session.CleanStart = connectPacket.CleanStart;
                        session.ExpiryInterval = effectiveExpiry;
                        session.DisconnectedAt = null;
                        session.WillDueAt = null;
                        session.WillMessage = connectPacket.HasWill
                            ? new MqttZeroWillMessage(connectPacket)
                            : null;
                    }
                }
            }
            catch
            {
                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            try
            {
                if (replacingSession)
                {
                    await DeleteClientPersistenceAsync(
                        clientId,
                        CancellationToken.None).ConfigureAwait(false);
                    existingPersistenceDeleted = true;
                }

                await SaveSessionAsync(session, CancellationToken.None).ConfigureAwait(false);
                newSessionPersisted = true;
                lock (_sessionGate)
                {
                    if (replacingSession)
                    {
                        if (!_sessions.TryGetValue(clientId, out MqttZeroSessionState? current) ||
                            !ReferenceEquals(current, replacedSession))
                        {
                            throw new InvalidOperationException(
                                "The MQTT session changed while the connection was being established.");
                        }

                        lock (replacedSession!.SyncRoot)
                        {
                            replacedSession.Connection = null;
                        }

                        _sessions[clientId] = session;
                    }
                    else if (!sessionPresent)
                    {
                        if (_sessions.Count >= _options.MaxSessions)
                        {
                            throw new InvalidOperationException(
                                "The maximum number of MQTT sessions was reached.");
                        }

                        _sessions[clientId] = session;
                    }
                }

            }
            catch
            {
                if (replacingSession)
                {
                    lock (_sessionGate)
                    {
                        if (_sessions.TryGetValue(clientId, out MqttZeroSessionState? current) &&
                            ReferenceEquals(current, session))
                        {
                            _sessions[clientId] = replacedSession!;
                        }
                    }

                    lock (replacedSession!.SyncRoot)
                    {
                        replacedSession.Connection = oldConnection;
                    }

                    RestoreSessionEgress(
                        previousPumpSession,
                        oldConnection,
                        previousPump,
                        previousPumpStopped);

                    if (existingPersistenceDeleted)
                    {
                        try
                        {
                            await RestoreSessionPersistenceAsync(
                                replacedSession,
                                CancellationToken.None).ConfigureAwait(false);
                        }
                        catch (Exception exception)
                        {
                            MqttZeroBrokerLogging.LogPersistenceFailure(
                                _logger,
                                nameof(RestoreSessionPersistenceAsync),
                                clientId,
                                exception);
                        }
                    }
                }
                else if (sessionPresent)
                {
                    lock (session.SyncRoot)
                    {
                        session.Connection = oldConnection;
                        session.CleanStart = previousCleanStart;
                        session.ExpiryInterval = previousExpiryInterval;
                        session.DisconnectedAt = previousDisconnectedAt;
                        session.WillMessage = previousWillMessage;
                        session.WillDueAt = previousWillDueAt;
                    }

                    RestoreSessionEgress(
                        previousPumpSession,
                        oldConnection,
                        previousPump,
                        previousPumpStopped);
                }
                else
                {
                    lock (_sessionGate)
                    {
                        _sessions.TryRemove(session.ClientId, out _);
                    }

                    ReleaseSessionMessages(session);
                    if (newSessionPersisted)
                    {
                        try
                        {
                            await DeleteSessionAsync(
                                clientId,
                                CancellationToken.None).ConfigureAwait(false);
                        }
                        catch (Exception exception)
                        {
                            MqttZeroBrokerLogging.LogPersistenceFailure(
                                _logger,
                                nameof(IMqttZeroSessionStore.DeleteAsync),
                                clientId,
                                exception);
                        }
                    }
                }

                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            try
            {
                await StopEgressAsync(previousPump).ConfigureAwait(false);
                previousPumpStopped = previousPump != null;
                if (takeoverWill != null)
                {
                    await RoutePublishAsync(null, takeoverWill.CreatePacket(), CancellationToken.None)
                        .ConfigureAwait(false);
                    Interlocked.Increment(ref _publishedWillCount);
                }

                await newConnection.SendConnAckAsync(
                    new MqttConnAckPacket
                    {
                        SessionPresent = sessionPresent,
                        ReasonCode = MqttReasonCode.Success,
                        TopicAliasMaximum = _options.TopicAliasMaximum,
                        ReceiveMaximum = (ushort)Math.Min(_options.MaxInflight, ushort.MaxValue),
                        MaximumPacketSize = (uint)_options.MaxPacketSize,
                        SessionExpiryInterval = effectiveExpiry,
                        AssignedClientIdentifier = assignedClientIdentifier ? clientId : null,
                        ResponseInformation =
                            connectPacket.RequestResponseInformation
                                ? _options.ResponseInformation
                                : null
                    },
                    CancellationToken.None).ConfigureAwait(false);
                newConnection.IsConnectedClientCounted = true;
                Interlocked.Increment(ref _connectedClientCount);
                if (oldConnection != null && !ReferenceEquals(oldConnection, newConnection))
                {
                    try
                    {
                        await oldConnection.CloseAsync(MqttReasonCode.SessionTakenOver, CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        MqttZeroBrokerLogging.LogConnectionCleanupFailure(_logger, exception);
                    }
                }

                if (replacedSession != null)
                {
                    ReleaseSessionMessages(replacedSession);
                }

                MqttZeroConnectedClient connectedClient = new(
                    this,
                    session,
                    newConnection);
                newConnection.ConnectedClient = connectedClient;
                MqttZeroClientConnectedContext connectedContext = new()
                {
                    ConnectedClient = connectedClient,
                    SessionPresent = sessionPresent
                };
                foreach (IMqttZeroConnectionObserver observer in _connectionObservers)
                {
                    try
                    {
                        await observer.OnConnectedAsync(connectedContext, CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
                    }
                }

                if (replacingSession && replacedSession != null)
                {
                    await RemoveSessionSubscriptionsFromRouteAsync(
                        replacedSession).ConfigureAwait(false);
                }

                return new ConnectionAttachment(session, newConnection);
            }
            catch
            {
                ReleaseConnectedClientCount(newConnection);
                if (replacingSession)
                {
                    lock (_sessionGate)
                    {
                        if (_sessions.TryGetValue(clientId, out MqttZeroSessionState? current) &&
                            ReferenceEquals(current, session))
                        {
                            _sessions[clientId] = replacedSession!;
                        }
                    }

                    lock (replacedSession!.SyncRoot)
                    {
                        replacedSession.Connection = oldConnection;
                    }

                    RestoreSessionEgress(
                        previousPumpSession,
                        oldConnection,
                        previousPump,
                        previousPumpStopped);

                    try
                    {
                        await RestoreSessionPersistenceAsync(
                            replacedSession,
                            CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        MqttZeroBrokerLogging.LogPersistenceFailure(
                            _logger,
                            nameof(RestoreSessionPersistenceAsync),
                            clientId,
                            exception);
                    }
                }
                else if (!sessionPresent)
                {
                    lock (_sessionGate)
                    {
                        _sessions.TryRemove(session.ClientId, out _);
                    }

                    ReleaseSessionMessages(session);
                    try
                    {
                        if (_sessionStore.IsEnabled)
                        {
                            await DeleteSessionAsync(
                                clientId,
                                CancellationToken.None).ConfigureAwait(false);
                        }
                    }
                    catch (Exception exception)
                    {
                        MqttZeroBrokerLogging.LogPersistenceFailure(
                            _logger,
                            nameof(IMqttZeroSessionStore.DeleteAsync),
                            clientId,
                            exception);
                    }
                }
                else
                {
                    lock (session.SyncRoot)
                    {
                        session.Connection = oldConnection;
                        session.CleanStart = previousCleanStart;
                        session.ExpiryInterval = previousExpiryInterval;
                        session.DisconnectedAt = previousDisconnectedAt;
                        session.WillMessage = previousWillMessage;
                        session.WillDueAt = previousWillDueAt;
                    }

                    RestoreSessionEgress(
                        previousPumpSession,
                        oldConnection,
                        previousPump,
                        previousPumpStopped);

                    try
                    {
                        await SaveSessionAsync(session, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        MqttZeroBrokerLogging.LogPersistenceFailure(
                            _logger,
                            nameof(SaveSessionAsync),
                            clientId,
                            exception);
                    }
                }

                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
    }

    private void RestoreSessionEgress(
        MqttZeroSessionState? session,
        MqttZeroClientConnection? connection,
        MqttZeroEgressActor? pump,
        bool pumpStopped)
    {
        if (session == null || connection == null)
        {
            return;
        }

        if (!pumpStopped && pump != null)
        {
            lock (session.SyncRoot)
            {
                if (!ReferenceEquals(session.Connection, connection) ||
                    session.Egress != null)
                {
                    return;
                }

                session.Egress = pump;
            }

            pump.Signal();
            return;
        }

        if (!connection.ClosedToken.IsCancellationRequested)
        {
            try
            {
                StartEgress(
                    session,
                    connection,
                    connection.ClosedToken);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogEgressFailure(
                    _logger,
                    session.ClientId,
                    exception);
                connection.Abort();
            }
        }
    }

    private void ReleaseConnectedClientCount(MqttZeroClientConnection connection)
    {
        if (!connection.IsConnectedClientCounted)
        {
            return;
        }

        connection.IsConnectedClientCounted = false;
        Interlocked.Decrement(ref _connectedClientCount);
    }

    private sealed class ConnectionAttachment
    {
        public ConnectionAttachment(
            MqttZeroSessionState session,
            MqttZeroClientConnection connection)
        {
            Session = session;
            Connection = connection;
        }

        public MqttZeroSessionState Session { get; }

        public MqttZeroClientConnection Connection { get; }
    }

    private async ValueTask<SessionMutationLease> AcquireSessionMutationAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        SessionMutationLock mutationLock;
        lock (_sessionGate)
        {
            if (!_sessionMutationLocks.TryGetValue(clientId, out mutationLock!))
            {
                mutationLock = new SessionMutationLock();
                _sessionMutationLocks.Add(clientId, mutationLock);
            }

            mutationLock.ReferenceCount++;
        }

        try
        {
            await mutationLock.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            ReleaseSessionMutationReference(clientId, mutationLock);
            throw;
        }

        return new SessionMutationLease(this, clientId, mutationLock);
    }

    private void ReleaseSessionMutation(SessionMutationLease lease)
    {
        lease.MutationLock.Gate.Release();
        ReleaseSessionMutationReference(lease.ClientId, lease.MutationLock);
    }

    private void ReleaseSessionMutationReference(
        string clientId,
        SessionMutationLock mutationLock)
    {
        lock (_sessionGate)
        {
            mutationLock.ReferenceCount--;
            if (mutationLock.ReferenceCount == 0 &&
                _sessionMutationLocks.TryGetValue(clientId, out SessionMutationLock? current) &&
                ReferenceEquals(current, mutationLock))
            {
                _sessionMutationLocks.Remove(clientId);
                mutationLock.Gate.Dispose();
            }
        }
    }

    private sealed class SessionMutationLock
    {
        public SemaphoreSlim Gate { get; } = new(1, 1);

        public int ReferenceCount { get; set; }
    }

    private sealed class SessionMutationLease : IDisposable
    {
        private readonly MqttZeroBroker _broker;
        private int _disposed;

        public SessionMutationLease(
            MqttZeroBroker broker,
            string clientId,
            SessionMutationLock mutationLock)
        {
            _broker = broker;
            ClientId = clientId;
            MutationLock = mutationLock;
        }

        public string ClientId { get; }

        public SessionMutationLock MutationLock { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _broker.ReleaseSessionMutation(this);
            }
        }
    }
}
