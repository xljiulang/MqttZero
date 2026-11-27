using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
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
    /// Processes one accepted connection until it disconnects.
    /// </summary>
    /// <param name="transport">The transport resources and connection metadata.</param>
    /// <param name="cancellationToken">A token that terminates the connection.</param>
    /// <returns>A task that completes when the connection closes.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="transport"/>, its input, or its output is null.
    /// </exception>
    public ValueTask AcceptConnectionAsync(
        MqttZeroTransportConnection transport,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(transport.Input);
        ArgumentNullException.ThrowIfNull(transport.Output);
        return AcceptConnectionCoreAsync(transport, cancellationToken);
    }

    private async ValueTask AcceptConnectionCoreAsync(
        MqttZeroTransportConnection transport,
        CancellationToken cancellationToken)
    {
        using AcceptedConnectionContext context = new(
            this,
            transport,
            cancellationToken);
        if (!TryAcceptConnection(context))
        {
            await context.Output.CompleteAsync().ConfigureAwait(false);
            return;
        }

        try
        {
            await RunConnectionAsync(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (InvalidOperationException) when (context.Session == null)
        {
        }
        finally
        {
            await CleanupAcceptedConnectionAsync(context).ConfigureAwait(false);
        }
    }

    private bool TryAcceptConnection(AcceptedConnectionContext context)
    {
        lock (_sessionGate)
        {
            if (!_options.AcceptNewConnections ||
                State != MqttZeroBrokerState.Running ||
                Volatile.Read(ref _activeConnectionCount) >=
                    _options.MaxActiveConnections)
            {
                return false;
            }

            Interlocked.Increment(ref _activeConnectionCount);
            _acceptedConnections[context.AcceptedConnectionId] =
                context.AcceptedCancellation;
            return true;
        }
    }

    private async ValueTask RunConnectionAsync(AcceptedConnectionContext context)
    {
        while (!context.ConnectionLifetime.IsCancellationRequested)
        {
            ReadResult readResult = await ReadConnectionBufferAsync(context)
                .ConfigureAwait(false);
            context.Buffer = readResult.Buffer;
            context.Examined = readResult.Buffer.End;

            if (MqttPacketCodec.TryGetPacketLength(
                    context.Buffer,
                    out long packetLength,
                    out _) &&
                packetLength > _options.MaxPacketSize)
            {
                throw new InvalidOperationException(
                    "The inbound MQTT packet exceeds the broker maximum.");
            }

            if (!await ProcessConnectionBufferAsync(context).ConfigureAwait(false))
            {
                return;
            }

            UpdateIncompletePacket(context, !context.Buffer.IsEmpty);

            if (readResult.IsCompleted)
            {
                context.Input.AdvanceTo(context.Buffer.Start);
                return;
            }

            context.Input.AdvanceTo(context.Buffer.Start, context.Examined);
        }
    }

    private async ValueTask<ReadResult> ReadConnectionBufferAsync(
        AcceptedConnectionContext context)
    {
        CancellationToken readToken = context.Session == null
            ? context.ConnectTimeout.Token
            : context.ConnectionLifetime.Token;
        using CancellationTokenSource? incompleteTimeout =
            context.Session != null && context.IncompleteSince.HasValue
                ? CancellationTokenSource.CreateLinkedTokenSource(readToken)
                : null;
        if (incompleteTimeout != null &&
            context.IncompleteSince is { } incompleteStart)
        {
            TimeSpan remaining =
                _options.IncompletePacketTimeout -
                (_timeProvider.GetUtcNow() - incompleteStart);
            if (remaining <= TimeSpan.Zero)
            {
                throw new OperationCanceledException(
                    "The incomplete MQTT packet timed out.");
            }

            incompleteTimeout.CancelAfter(remaining);
            readToken = incompleteTimeout.Token;
        }

        return await context.Input.ReadAsync(readToken).ConfigureAwait(false);
    }

    private async ValueTask<bool> ProcessConnectionBufferAsync(
        AcceptedConnectionContext context)
    {
        while (!context.Buffer.IsEmpty)
        {
            MqttZeroClientConnection? connection = context.Connection;
            MqttZeroSessionState? session = context.Session;
            if (session != null &&
                connection != null &&
                (context.Buffer.Slice(0, 1).FirstSpan[0] >> 4) == 0x0F)
            {
                await connection.CloseAsync(
                    MqttReasonCode.ProtocolError,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
                return false;
            }

            MqttZeroInboundBufferAction publishAction =
                await ProcessFastPublishAsync(context).ConfigureAwait(false);
            if (publishAction == MqttZeroInboundBufferAction.Stop)
            {
                return false;
            }

            if (publishAction == MqttZeroInboundBufferAction.Handled)
            {
                continue;
            }

            MqttZeroInboundBufferAction acknowledgementAction =
                await ProcessAcknowledgementAsync(context).ConfigureAwait(false);
            if (acknowledgementAction == MqttZeroInboundBufferAction.Stop)
            {
                return false;
            }

            if (acknowledgementAction == MqttZeroInboundBufferAction.Handled)
            {
                continue;
            }

            if (!MqttPacketCodec.TryDecode(
                    context.Buffer,
                    out MqttPacket? packet,
                    out long consumed) ||
                packet == null)
            {
                return true;
            }

            if (context.Session == null)
            {
                if (!await ProcessConnectPacketAsync(context, packet, consumed)
                    .ConfigureAwait(false))
                {
                    return false;
                }

                continue;
            }

            if (!await ProcessDecodedPacketAsync(context, packet, consumed)
                .ConfigureAwait(false))
            {
                return false;
            }
        }

        return true;
    }

    private async ValueTask<MqttZeroInboundBufferAction> ProcessFastPublishAsync(
        AcceptedConnectionContext context)
    {
        if (!context.Buffer.IsEmpty)
        {
            byte fixedHeader =
                context.Buffer.Slice(0, 1).FirstSpan[0];
            bool isQosZeroPublish =
                (fixedHeader >> 4) == 0x03 &&
                (fixedHeader & 0x06) == 0;
            if (isQosZeroPublish &&
                context.PendingPublishCount >=
                    AcceptedConnectionContext.
                        MaximumPendingPublishesInFlight)
            {
                await context.WaitForPendingPublishesAsync()
                    .ConfigureAwait(false);
            }
        }

        if (context.Session == null ||
            context.Connection == null ||
            !MqttPublishPacketCodec.TryDecode(
                context.Buffer,
                out MqttZeroPublishView publishView,
                out long publishConsumed))
        {
            return MqttZeroInboundBufferAction.NotHandled;
        }

        MqttZeroClientConnection connection = context.Connection;
        ushort publishPacketIdentifier =
            publishView.PacketIdentifier;
        connection.MarkPacketReceived(
            _timeProvider,
            publishConsumed,
            true);
        try
        {
            if (!TryCreateMessageLease(
                    connection,
                    publishView,
                    out MqttZeroMessageLease? candidate,
                    out MqttReasonCode reasonCode))
            {
                bool stop = await RejectInvalidPublishAsync(
                    connection,
                    publishView.QualityOfServiceLevel,
                    publishPacketIdentifier,
                    reasonCode,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
                context.Buffer = context.Buffer.Slice(publishConsumed);
                return stop
                    ? MqttZeroInboundBufferAction.Stop
                    : MqttZeroInboundBufferAction.Handled;
            }

            context.CurrentOwnedPublishLease = candidate;
            context.CurrentOwnedPublishPacketIdentifier =
                publishPacketIdentifier;
            if (candidate.QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtMostOnce)
            {
                await OwnInboundLeaseAsync(
                    candidate,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
                try
                {
                    if (CanRouteQosZeroDirectly(candidate))
                    {
                        await RoutePublishLeaseFastAsync(
                            context.Session!,
                            connection,
                            candidate,
                            suppressPublisherAck: false,
                            cancellationToken:
                                context.ConnectionLifetime.Token,
                            scheduleQosZeroWithoutWaiting: true)
                            .ConfigureAwait(false);
                        context.Buffer =
                            context.Buffer.Slice(publishConsumed);
                        return MqttZeroInboundBufferAction.Handled;
                    }

                    MqttZeroEventLoop shard =
                        TryPrepareFastPublish(
                        context,
                        context.Session,
                        connection,
                        candidate,
                        publishPacketIdentifier,
                        out MqttZeroInboundShardWork shardWork);
                    if (!shard.TryEnqueue(shardWork))
                    {
                        try
                        {
                            await shard.EnqueueAsync(
                                shardWork,
                                context.ConnectionLifetime.Token)
                                .ConfigureAwait(false);
                        }
                        catch
                        {
                            shardWork.Release();
                            throw;
                        }
                    }
                }
                finally
                {
                    candidate.Release();
                    context.CurrentOwnedPublishLease = null;
                }

                context.Buffer = context.Buffer.Slice(publishConsumed);
                return MqttZeroInboundBufferAction.Handled;
            }

            if (candidate.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce &&
                CanDispatchInbound)
            {
                await OwnInboundLeaseAsync(
                    candidate,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
                MqttZeroInboundBufferAction qosOneAction =
                    await ProcessQosOnePublishDirectAsync(
                        context.Session!,
                        connection,
                        candidate,
                        context.ConnectionLifetime.Token)
                        .ConfigureAwait(false);
                context.CurrentOwnedPublishLease = null;
                if (qosOneAction == MqttZeroInboundBufferAction.Stop)
                {
                    return MqttZeroInboundBufferAction.Stop;
                }

                context.Buffer = context.Buffer.Slice(publishConsumed);
                return MqttZeroInboundBufferAction.Handled;
            }

            MqttZeroInboundBufferAction action;
            try
            {
                action = await context.InvokeShardCommandAsync(
                    ShardSet[context.Session.ShardId],
                    context.CachedOwnedPublishInvocation,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
            }
            finally
            {
                context.CurrentOwnedPublishLease = null;
            }
            if (action == MqttZeroInboundBufferAction.Stop)
            {
                return MqttZeroInboundBufferAction.Stop;
            }

            context.Buffer = context.Buffer.Slice(publishConsumed);
            return MqttZeroInboundBufferAction.Handled;
        }
        catch (MqttZeroBufferPoolExhaustedException)
        {
            await connection.CloseAsync(
                MqttReasonCode.QuotaExceeded,
                context.ConnectionLifetime.Token).ConfigureAwait(false);
            return MqttZeroInboundBufferAction.Stop;
        }
    }

    private static async ValueTask<bool> RejectInvalidPublishAsync(
        MqttZeroClientConnection connection,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        if (qualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtLeastOnce &&
            reasonCode == MqttReasonCode.PayloadFormatInvalid)
        {
            await connection.SendPubAckAsync(
                packetIdentifier,
                reasonCode,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (qualityOfServiceLevel ==
                MqttQualityOfServiceLevel.ExactlyOnce &&
            reasonCode == MqttReasonCode.PayloadFormatInvalid)
        {
            await connection.SendPubRecAsync(
                packetIdentifier,
                reasonCode,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        await connection.CloseAsync(
            reasonCode,
            cancellationToken).ConfigureAwait(false);
        return true;
    }

    private async ValueTask<MqttZeroInboundBufferAction> ProcessOwnedPublishAsync(
        AcceptedConnectionContext context,
        MqttZeroMessageLease lease,
        ushort inboundPacketIdentifier,
        CancellationToken cancellationToken)
    {
        await OwnInboundLeaseAsync(lease, cancellationToken).ConfigureAwait(false);
        return await ProcessOwnedPublishCoreAsync(
            context.Session!,
            context.Connection!,
            lease,
            inboundPacketIdentifier,
            releaseLease: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<MqttZeroInboundBufferAction>
        ProcessQosOnePublishDirectAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        bool transferred = false;
        try
        {
            PublishDispatchResult result =
                await DispatchQosOnePublishAsync(
                    session,
                    connection,
                    lease,
                    cancellationToken).ConfigureAwait(false);
            transferred = result == PublishDispatchResult.Transferred;
            return result == PublishDispatchResult.Stop
                ? MqttZeroInboundBufferAction.Stop
                : MqttZeroInboundBufferAction.Handled;
        }
        finally
        {
            if (!transferred)
            {
                lease.Release();
            }
        }
    }

    private async ValueTask<MqttZeroInboundBufferAction>
        ProcessOwnedPublishCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        ushort inboundPacketIdentifier,
        bool releaseLease,
        CancellationToken cancellationToken)
    {
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
                    inboundPacketIdentifier,
                    releaseSourceLease: releaseLease,
                    cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
            else if (lease.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce &&
                CanDispatchInbound)
            {
                PublishDispatchResult result = await DispatchQosOnePublishAsync(
                    session,
                    connection,
                    lease,
                    cancellationToken).ConfigureAwait(false);
                transferred = result == PublishDispatchResult.Transferred;
                if (result == PublishDispatchResult.Stop)
                {
                    return MqttZeroInboundBufferAction.Stop;
                }
            }
            else
            {
                await RoutePublishLeaseAsync(
                    session,
                    connection,
                    lease,
                    cancellationToken).ConfigureAwait(false);
            }

            return MqttZeroInboundBufferAction.Handled;
        }
        finally
        {
            if (releaseLease && !transferred)
            {
                lease.Release();
            }
        }
    }

    private MqttZeroEventLoop TryPrepareFastPublish(
        AcceptedConnectionContext context,
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        ushort packetIdentifier,
        out MqttZeroInboundShardWork shardWork)
    {
        TaskCompletionSource completion = context.BeginPendingPublish();
        MqttZeroInboundWork work = MqttZeroInboundWork.Rent(
            lease,
            session,
            connection,
            packetIdentifier,
            context.CachedPendingPublishCompleted,
            completion);
        try
        {
            shardWork = MqttZeroInboundShardWork.Rent(
                work,
                context.CachedPendingPublishInvocation,
                context.CachedInboundShardFailure);
        }
        catch
        {
            work.Release();
            throw;
        }

        return ShardSet[session.ShardId];
    }

    private bool CanRouteQosZeroDirectly(MqttZeroMessageLease lease)
    {
        return !lease.RetainFlag &&
            !_sessionStore.IsEnabled &&
            !_messageStore.IsEnabled &&
            !_retainedMessageStore.IsEnabled &&
            _publishInterceptors.Length == 0 &&
            _enqueueInterceptors.Length == 0 &&
            _deliveryObservers.Length == 0;
    }

    private async ValueTask ProcessQueuedFastPublishAsync(
        MqttZeroInboundWork work,
        CancellationToken cancellationToken)
    {
        MqttZeroClientConnection? connection = work.SenderConnection;
        try
        {
            MqttZeroInboundBufferAction action =
                await ProcessOwnedPublishCoreAsync(
                    work.Sender,
                    connection ??
                        throw new InvalidOperationException(
                            "The fast publisher connection is unavailable."),
                    work.Lease,
                    checked((ushort)work.MessageId),
                    releaseLease: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            if (action == MqttZeroInboundBufferAction.Stop)
            {
                await connection.CloseAsync(
                    MqttReasonCode.UnspecifiedError,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            if (connection != null &&
                !cancellationToken.IsCancellationRequested)
            {
                await connection.CloseAsync(
                    MqttReasonCode.UnspecifiedError,
                    CancellationToken.None).ConfigureAwait(false);
            }

            throw;
        }
    }

    private async ValueTask EnqueueAcknowledgementAsync(
        AcceptedConnectionContext context,
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroAcknowledgementView acknowledgement,
        CancellationToken cancellationToken)
    {
        TaskCompletionSource completion = context.BeginAcknowledgement();
        MqttZeroAcknowledgementWork work =
            MqttZeroAcknowledgementWork.Rent(
                session,
                connection,
                acknowledgement,
                context.CachedAcknowledgementCompleted,
                completion);
        MqttZeroAcknowledgementShardWork shardWork =
            MqttZeroAcknowledgementShardWork.Rent(
                work,
                context.CachedAcknowledgementInvocation,
                context.CachedInboundShardFailure);
        MqttZeroEventLoop shard = ShardSet[session.ShardId];
        try
        {
            if (!shard.TryEnqueue(shardWork))
            {
                await shard.EnqueueAsync(
                    shardWork,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            shardWork.Release();
            throw;
        }
    }

    private async ValueTask ProcessQueuedAcknowledgementAsync(
        MqttZeroAcknowledgementWork work,
        CancellationToken cancellationToken)
    {
        try
        {
            await ProcessAcknowledgementCoreAsync(
                work.Session,
                work.Connection,
                work.Acknowledgement,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                await work.Connection.CloseAsync(
                    MqttReasonCode.UnspecifiedError,
                    CancellationToken.None).ConfigureAwait(false);
            }

            throw;
        }
    }

    private async ValueTask<PublishDispatchResult> DispatchQosOnePublishAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        MqttZeroPreparedPublish prepared =
            await PreparePublishForDispatchAsync(
                session,
                connection,
                lease,
                cancellationToken).ConfigureAwait(false);
        if (prepared.Result == MqttZeroInterceptionResult.CloseConnection)
        {
            await connection.CloseAsync(
                prepared.ReasonCode == MqttReasonCode.Success
                    ? MqttReasonCode.NotAuthorized
                    : prepared.ReasonCode,
                cancellationToken).ConfigureAwait(false);
            return PublishDispatchResult.Stop;
        }

        if (prepared.Result != MqttZeroInterceptionResult.Continue ||
            prepared.Lease == null)
        {
            await connection.SendPubAckAsync(
                lease.PacketIdentifier,
                prepared.ReasonCode,
                cancellationToken).ConfigureAwait(false);
            return PublishDispatchResult.NotTransferred;
        }

        long messageId = _messageStore.IsEnabled
            ? GetNextMessageId()
            : 0;
        try
        {
            await SaveInboundDispatchAsync(
                prepared.Lease,
                session,
                messageId,
                cancellationToken).ConfigureAwait(false);
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
                cancellationToken,
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

        if (!prepared.UsesSourceLease)
        {
            lease.Release();
        }

        prepared.Lease.Release();
        await connection.SendPubAckAsync(
            lease.PacketIdentifier,
            MqttReasonCode.Success,
            cancellationToken).ConfigureAwait(false);
        return PublishDispatchResult.Transferred;
    }

    private async ValueTask<MqttZeroInboundBufferAction> ProcessAcknowledgementAsync(
        AcceptedConnectionContext context)
    {
        if (context.Session == null ||
            context.Connection == null ||
            context.Buffer.IsEmpty ||
            !MqttPacketCodec.IsAcknowledgementPacketType(
                (MqttPacketType)(context.Buffer.Slice(0, 1).FirstSpan[0] >> 4)))
        {
            return MqttZeroInboundBufferAction.NotHandled;
        }

        MqttZeroClientConnection connection = context.Connection;
        MqttZeroSessionState session = context.Session;
        if (MqttPacketCodec.TryDecodeAcknowledgement(
                context.Buffer,
                out MqttZeroAcknowledgementView acknowledgement,
                out long acknowledgementConsumed))
        {
            connection.MarkPacketReceived(
                _timeProvider,
                acknowledgementConsumed,
                false);
            if (acknowledgement.PacketType == MqttPacketType.PubAck)
            {
                await EnqueueAcknowledgementAsync(
                    context,
                    session,
                    connection,
                    acknowledgement,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
            }
            else
            {
                context.CurrentAcknowledgement = acknowledgement;
                try
                {
                    await context.InvokeShardCommandAsync(
                        ShardSet[session.ShardId],
                        context.CachedSynchronousAcknowledgementInvocation,
                        context.ConnectionLifetime.Token).ConfigureAwait(false);
                }
                finally
                {
                    context.CurrentAcknowledgement = default;
                }
            }

            context.Buffer = context.Buffer.Slice(acknowledgementConsumed);
            return MqttZeroInboundBufferAction.Handled;
        }

        if (MqttPacketCodec.TryGetPacketLength(
                context.Buffer,
                out _,
                out bool acknowledgementComplete) &&
            acknowledgementComplete)
        {
            await connection.CloseAsync(
                MqttReasonCode.ProtocolError,
                context.ConnectionLifetime.Token).ConfigureAwait(false);
            return MqttZeroInboundBufferAction.Stop;
        }

        return MqttZeroInboundBufferAction.NotHandled;
    }

    private async ValueTask ProcessAcknowledgementCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroAcknowledgementView acknowledgement,
        CancellationToken cancellationToken)
    {
        MqttZeroShardOwnership.EnsureCurrent(
            session.ShardId,
            nameof(ProcessAcknowledgementCoreAsync),
            _fanoutDiagnostics);
        switch (acknowledgement.PacketType)
        {
            case MqttPacketType.PubAck:
                await ProcessOutboundPubAckAsync(
                    session,
                    connection,
                    acknowledgement.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPacketType.PubRec:
                await ProcessOutboundPubRecAsync(
                    session,
                    connection,
                    acknowledgement.PacketIdentifier,
                    acknowledgement.ReasonCode,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPacketType.PubRel:
                await ProcessInboundPubRelAsync(
                    session,
                    connection,
                    acknowledgement.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPacketType.PubComp:
                await ProcessOutboundPubCompAsync(
                    session,
                    connection,
                    acknowledgement.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
        }
    }

    private async ValueTask<bool> ProcessConnectPacketAsync(
        AcceptedConnectionContext context,
        MqttPacket packet,
        long consumed)
    {
        if (packet is not MqttConnectPacket connectPacket)
        {
            return false;
        }

        context.CurrentConnectPacket = connectPacket;
        context.AssignedClientIdentifier = connectPacket.ClientId.IsEmpty;
        context.ConnectClientId = context.AssignedClientIdentifier
            ? $"mqttzero-{Guid.NewGuid():N}"
            : Encoding.UTF8.GetString(connectPacket.ClientId.ToArray());
        context.SessionShardId = SessionShardMap.GetShardId(
            context.ConnectClientId);
        ConnectionAttachment? attachment = await ShardSet[context.SessionShardId]
            .InvokeAsync(
                context.CachedConnectInvocation,
                context.ConnectionLifetime.Token)
            .ConfigureAwait(false);
        context.CurrentConnectPacket = null;
        if (attachment == null)
        {
            return false;
        }

        context.Session = attachment.Session;
        context.Connection = attachment.Connection;
        context.CloseRegistration = attachment.Connection.ClosedToken.Register(
            CancelAcceptedConnection,
            context.ConnectionLifetime);
        await ShardSet[context.SessionShardId].InvokeAsync(
            _ =>
            {
                StartEgress(
                    attachment.Session,
                    attachment.Connection,
                    context.ConnectionLifetime.Token);
                return ValueTask.FromResult(true);
            },
            context.ConnectionLifetime.Token).ConfigureAwait(false);
        context.Buffer = context.Buffer.Slice(consumed);
        return true;
    }

    private async ValueTask<bool> ProcessDecodedPacketAsync(
        AcceptedConnectionContext context,
        MqttPacket packet,
        long consumed)
    {
        if (context.Connection == null || context.Session == null)
        {
            return false;
        }

        context.CurrentPacket = packet;
        context.CurrentPacketConsumed = consumed;
        await ShardSet[context.Connection.ShardId]
            .InvokeAsync(
                context.CachedPacketInvocation,
                context.ConnectionLifetime.Token)
            .ConfigureAwait(false);
        context.CurrentPacket = null;
        context.Buffer = context.Buffer.Slice(consumed);

        if (packet is MqttDisconnectPacket)
        {
            context.CleanDisconnect = true;
            context.Connection.DisconnectType =
                MqttZeroDisconnectType.ClientRequest;
            return false;
        }

        return true;
    }

    private void UpdateIncompletePacket(
        AcceptedConnectionContext context,
        bool incompletePacket)
    {
        if (context.Connection != null)
        {
            context.Connection.IsReadingIncompletePacket = incompletePacket;
        }

        if (!incompletePacket)
        {
            context.IncompleteSince = null;
            return;
        }

        context.IncompleteSince ??= _timeProvider.GetUtcNow();
    }

    private async ValueTask CleanupAcceptedConnectionAsync(
        AcceptedConnectionContext context)
    {
        context.CloseRegistration.Dispose();
        try
        {
            await context.WaitForPendingPublishesAsync()
                .ConfigureAwait(false);
            await context.WaitForAcknowledgementsAsync()
                .ConfigureAwait(false);
            await DetachAcceptedConnectionAsync(context).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogConnectionCleanupFailure(_logger, exception);
        }
        finally
        {
            await ReleaseAcceptedConnectionAsync(context).ConfigureAwait(false);
        }
    }

    private async ValueTask DetachAcceptedConnectionAsync(
        AcceptedConnectionContext context)
    {
        try
        {
            if (context.Connection != null)
            {
                ReleaseConnectedClientCount(context.Connection);
            }

            if (context.Session == null || context.Connection == null)
            {
                return;
            }

            context.Connection.IsReadingIncompletePacket = false;
            await DetachConnectionAsync(
                context.Session,
                context.Connection,
                context.CleanDisconnect).ConfigureAwait(false);
            await NotifyDisconnectedAsync(context.Connection).ConfigureAwait(false);
        }
        finally
        {
            if (context.Connection != null && !context.ConnectionDisposed)
            {
                context.ConnectionDisposed = true;
                await context.Connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private async ValueTask NotifyDisconnectedAsync(
        MqttZeroClientConnection connection)
    {
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

    private async ValueTask ReleaseAcceptedConnectionAsync(
        AcceptedConnectionContext context)
    {
        _acceptedConnections.TryRemove(context.AcceptedConnectionId, out _);
        int remaining = Interlocked.Decrement(ref _activeConnectionCount);
        if (remaining == 0)
        {
            _connectionsDrained.TrySetResult();
        }

        try
        {
            await context.Input.CompleteAsync().ConfigureAwait(false);
        }
        finally
        {
            if (context.Connection == null)
            {
                await context.Output.CompleteAsync().ConfigureAwait(false);
            }
        }
    }

    private enum PublishDispatchResult
    {
        NotTransferred,
        Transferred,
        Stop
    }

    private sealed class AcceptedConnectionContext : IDisposable
    {
        public const int MaximumPendingPublishesInFlight = 16;

        private readonly MqttZeroBroker _broker;
        private readonly MqttZeroShardCommand<
            MqttZeroInboundBufferAction> _shardCommand = new();
        private readonly MqttZeroPendingDrain _acknowledgementDrain = new();
        private readonly MqttZeroPendingDrain _pendingPublishDrain = new();

        public AcceptedConnectionContext(
            MqttZeroBroker broker,
            MqttZeroTransportConnection transport,
            CancellationToken cancellationToken)
        {
            _broker = broker;
            Input = transport.Input;
            Output = transport.Output;
            RemoteEndPoint = transport.RemoteEndPoint;
            IsSecure = transport.IsSecure;
            IsWebSocket = transport.IsWebSocket;
            ClientCertificate = transport.ClientCertificate;
            AllowPacketFragmentation = transport.AllowPacketFragmentation;
            AcceptedConnectionId = Interlocked.Increment(
                ref _broker._nextAcceptedConnectionId);
            AcceptedCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            ConnectionLifetime = CancellationTokenSource.CreateLinkedTokenSource(
                AcceptedCancellation.Token);
            ConnectTimeout = CancellationTokenSource.CreateLinkedTokenSource(
                ConnectionLifetime.Token);
            ConnectTimeout.CancelAfter(_broker._options.ConnectTimeout);
            CachedConnectInvocation = InvokeConnectAsync;
            CachedPacketInvocation = InvokePacketOnShardAsync;
            CachedOwnedPublishInvocation =
                InvokeOwnedPublishOnShardAsync;
            CachedPendingPublishCompleted =
                CompletePendingPublish;
            CachedPendingPublishInvocation =
                broker.ProcessQueuedFastPublishAsync;
            CachedInboundShardFailure =
                broker.HandleConnectionWorkFailure;
            CachedAcknowledgementCompleted =
                CompleteAcknowledgement;
            CachedAcknowledgementInvocation =
                broker.ProcessQueuedAcknowledgementAsync;
            CachedSynchronousAcknowledgementInvocation =
                InvokeAcknowledgementOnShardAsync;
        }

        public PipeReader Input { get; }

        public PipeWriter Output { get; }

        public EndPoint? RemoteEndPoint { get; }

        public bool IsSecure { get; }

        public bool IsWebSocket { get; }

        public X509Certificate2? ClientCertificate { get; }

        public bool AllowPacketFragmentation { get; }

        public long AcceptedConnectionId { get; }

        public CancellationTokenSource AcceptedCancellation { get; }

        public CancellationTokenSource ConnectionLifetime { get; }

        public CancellationTokenSource ConnectTimeout { get; }

        public MqttZeroClientConnection? Connection { get; set; }

        public MqttZeroSessionState? Session { get; set; }

        public CancellationTokenRegistration CloseRegistration { get; set; }

        public bool ConnectionDisposed { get; set; }

        public bool CleanDisconnect { get; set; }

        public DateTimeOffset? IncompleteSince { get; set; }

        public ReadOnlySequence<byte> Buffer { get; set; }

        public SequencePosition Examined { get; set; }

        public MqttConnectPacket? CurrentConnectPacket { get; set; }

        public string ConnectClientId { get; set; } = string.Empty;

        public bool AssignedClientIdentifier { get; set; }

        public int SessionShardId { get; set; }

        public MqttPacket? CurrentPacket { get; set; }

        public long CurrentPacketConsumed { get; set; }

        public Func<CancellationToken, ValueTask<ConnectionAttachment?>>
            CachedConnectInvocation
        { get; }

        public Func<CancellationToken, ValueTask<bool>>
            CachedPacketInvocation
        { get; }

        public Func<
            CancellationToken,
            ValueTask<MqttZeroInboundBufferAction>>
            CachedOwnedPublishInvocation
        { get; }

        public Action<TaskCompletionSource> CachedPendingPublishCompleted { get; }

        public Func<
            MqttZeroInboundWork,
            CancellationToken,
            ValueTask>
            CachedPendingPublishInvocation
        { get; }

        public Action<Exception> CachedInboundShardFailure { get; }

        public Action<TaskCompletionSource> CachedAcknowledgementCompleted { get; }

        public Func<
            MqttZeroAcknowledgementWork,
            CancellationToken,
            ValueTask>
            CachedAcknowledgementInvocation
        { get; }

        public Func<
            CancellationToken,
            ValueTask<MqttZeroInboundBufferAction>>
            CachedSynchronousAcknowledgementInvocation
        { get; }

        public MqttZeroMessageLease? CurrentOwnedPublishLease
        { get; set; }

        public ushort CurrentOwnedPublishPacketIdentifier
        { get; set; }

        public MqttZeroAcknowledgementView CurrentAcknowledgement
        { get; set; }

        public int PendingPublishCount
        {
            get
            {
                return _pendingPublishDrain.Count;
            }
        }

        public TaskCompletionSource BeginPendingPublish()
        {
            return _pendingPublishDrain.Begin();
        }

        public void CompletePendingPublish(TaskCompletionSource token)
        {
            _pendingPublishDrain.Complete(token);
        }

        public ValueTask WaitForPendingPublishesAsync()
        {
            return _pendingPublishDrain.WaitAsync();
        }

        public TaskCompletionSource BeginAcknowledgement()
        {
            return _acknowledgementDrain.Begin();
        }

        public void CompleteAcknowledgement(TaskCompletionSource token)
        {
            _acknowledgementDrain.Complete(token);
        }

        public ValueTask WaitForAcknowledgementsAsync()
        {
            return _acknowledgementDrain.WaitAsync();
        }

        public ValueTask<MqttZeroInboundBufferAction>
            InvokeShardCommandAsync(
            MqttZeroEventLoop shard,
            Func<
                CancellationToken,
                ValueTask<MqttZeroInboundBufferAction>> operation,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(shard);
            ArgumentNullException.ThrowIfNull(operation);
            if (shard.IsCurrent)
            {
                return operation(cancellationToken);
            }

            _shardCommand.Prepare(operation, cancellationToken);
            return shard.InvokeAsync(
                _shardCommand,
                cancellationToken);
        }

        public void Dispose()
        {
            ConnectTimeout.Dispose();
            ConnectionLifetime.Dispose();
            AcceptedCancellation.Dispose();
        }

        private ValueTask<ConnectionAttachment?> InvokeConnectAsync(
            CancellationToken cancellationToken)
        {
            MqttConnectPacket connectPacket = CurrentConnectPacket ??
                throw new InvalidOperationException(
                    "The CONNECT packet is not available.");
            return _broker.ConnectAsync(
                connectPacket,
                Output,
                RemoteEndPoint,
                IsSecure,
                IsWebSocket,
                ClientCertificate,
                AllowPacketFragmentation,
                ConnectClientId,
                AssignedClientIdentifier,
                SessionShardId,
                cancellationToken);
        }

        private async ValueTask<bool> InvokePacketOnShardAsync(
            CancellationToken cancellationToken)
        {
            MqttPacket packet = CurrentPacket ??
                throw new InvalidOperationException(
                    "The MQTT packet is not available.");
            MqttZeroClientConnection connection = Connection ??
                throw new InvalidOperationException(
                    "The MQTT connection is not available.");
            MqttZeroSessionState session = Session ??
                throw new InvalidOperationException(
                    "The MQTT session is not available.");
            connection.MarkPacketReceived(
                _broker._timeProvider,
                CurrentPacketConsumed,
                packet is MqttPublishPacket);
            await _broker.ProcessPacketAsync(
                session,
                connection,
                packet,
                cancellationToken).ConfigureAwait(false);
            return true;
        }

        private ValueTask<MqttZeroInboundBufferAction>
            InvokeOwnedPublishOnShardAsync(
            CancellationToken cancellationToken)
        {
            MqttZeroMessageLease lease =
                CurrentOwnedPublishLease ??
                throw new InvalidOperationException(
                    "The owned PUBLISH lease is not available.");
            return _broker.ProcessOwnedPublishAsync(
                this,
                lease,
                CurrentOwnedPublishPacketIdentifier,
                cancellationToken);
        }

        private ValueTask<MqttZeroInboundBufferAction>
            InvokeAcknowledgementOnShardAsync(
            CancellationToken cancellationToken)
        {
            MqttZeroClientConnection connection = Connection ??
                throw new InvalidOperationException(
                    "The MQTT connection is not available.");
            MqttZeroSessionState session = Session ??
                throw new InvalidOperationException(
                    "The MQTT session is not available.");
            return InvokeAcknowledgementCoreAsync(
                session,
                connection,
                CurrentAcknowledgement,
                cancellationToken);
        }

        private async ValueTask<MqttZeroInboundBufferAction>
            InvokeAcknowledgementCoreAsync(
            MqttZeroSessionState session,
            MqttZeroClientConnection connection,
            MqttZeroAcknowledgementView acknowledgement,
            CancellationToken cancellationToken)
        {
            await _broker.ProcessAcknowledgementCoreAsync(
                session,
                connection,
                acknowledgement,
                cancellationToken).ConfigureAwait(false);
            return MqttZeroInboundBufferAction.Handled;
        }

    }

    private sealed class ConnectTransaction
    {
        public ConnectTransaction(
            MqttZeroBroker broker,
            MqttConnectPacket connectPacket,
            PipeWriter output,
            EndPoint? remoteEndPoint,
            bool isSecure,
            bool isWebSocket,
            X509Certificate2? clientCertificate,
            bool allowPacketFragmentation,
            string clientId,
            bool assignedClientIdentifier,
            int sessionShardId)
        {
            ConnectPacket = connectPacket;
            ClientId = clientId;
            AssignedClientIdentifier = assignedClientIdentifier;
            EffectiveExpiry = broker.ClampSessionExpiry(
                connectPacket.SessionExpiryInterval ?? 0);
            Now = broker._timeProvider.GetUtcNow();
            NewConnection = new MqttZeroClientConnection(
                output,
                remoteEndPoint,
                isSecure,
                isWebSocket,
                connectPacket.KeepAliveSeconds,
                connectPacket.ReceiveMaximum,
                connectPacket.MaximumPacketSize,
                broker._timeProvider,
                broker._bufferPool,
                allowPacketFragmentation,
                connectPacket.RequestProblemInformation,
                connectPacket.RequestResponseInformation,
                sessionShardId);
            ClientInfo = new MqttZeroClientConnectionInfo
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
            NewConnection.ClientInfo = ClientInfo;
        }

        public MqttConnectPacket ConnectPacket { get; }

        public string ClientId { get; }

        public bool AssignedClientIdentifier { get; }

        public uint EffectiveExpiry { get; }

        public DateTimeOffset Now { get; }

        public MqttZeroClientConnection NewConnection { get; }

        public MqttZeroClientConnectionInfo ClientInfo { get; }

        public MqttZeroSessionState Session { get; set; } = null!;

        public bool SessionPresent { get; set; }

        public bool ReplacingSession { get; set; }

        public bool NewSessionPersisted { get; set; }

        public bool ExistingPersistenceDeleted { get; set; }

        public MqttZeroSessionState? ReplacedSession { get; set; }

        public bool PreviousCleanStart { get; set; }

        public uint PreviousExpiryInterval { get; set; }

        public DateTimeOffset? PreviousDisconnectedAt { get; set; }

        public MqttZeroWillMessage? PreviousWillMessage { get; set; }

        public DateTimeOffset? PreviousWillDueAt { get; set; }

        public MqttZeroSessionState? PreviousPumpSession { get; set; }

        public MqttZeroSessionEgress? PreviousPump { get; set; }

        public bool PreviousPumpStopped { get; set; }

        public MqttZeroClientConnection? OldConnection { get; set; }

        public MqttZeroWillMessage? TakeoverWill { get; set; }
    }

    private async ValueTask<ConnectionAttachment?> ConnectAsync(
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
        MqttZeroShardOwnership.EnsureCurrent(
            sessionShardId,
            nameof(ConnectAsync),
            _fanoutDiagnostics);
        ConnectTransaction transaction = new(
            this,
            connectPacket,
            output,
            remoteEndPoint,
            isSecure,
            isWebSocket,
            clientCertificate,
            allowPacketFragmentation,
            clientId,
            assignedClientIdentifier,
            sessionShardId);
        MqttZeroClientConnection newConnection = transaction.NewConnection;
        if (!await PrepareConnectAsync(transaction).ConfigureAwait(false))
        {
            return null;
        }

        try
        {
            try
            {
                AttachSession(transaction);
            }
            catch
            {
                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            try
            {
                await PersistSessionAsync(transaction).ConfigureAwait(false);
            }
            catch
            {
                await RollbackTransactionAsync(
                    transaction,
                    registerDeadlines: true,
                    removeDeadlines: false,
                    persistRestoredSession: false).ConfigureAwait(false);

                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            try
            {
                await ActivateConnectionAsync(transaction).ConfigureAwait(false);
                return new ConnectionAttachment(
                    transaction.Session,
                    transaction.NewConnection);
            }
            catch
            {
                ReleaseConnectedClientCount(newConnection);
                await RollbackTransactionAsync(
                    transaction,
                    registerDeadlines: false,
                    removeDeadlines: true,
                    persistRestoredSession: true).ConfigureAwait(false);

                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            await newConnection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async ValueTask<bool> PrepareConnectAsync(
        ConnectTransaction transaction)
    {
        MqttConnectPacket connectPacket = transaction.ConnectPacket;
        if (connectPacket.AuthenticationMethod != null)
        {
            return await RejectConnectAsync(
                transaction,
                MqttReasonCode.BadAuthenticationMethod).ConfigureAwait(false);
        }

        if (connectPacket.HasWill &&
            connectPacket.WillProperties.PayloadFormatIndicator ==
                MqttPayloadFormatIndicator.Utf8 &&
            !MqttZeroProtocolValidator.IsValidUtf8(connectPacket.WillPayload))
        {
            return await RejectConnectAsync(
                transaction,
                MqttReasonCode.PayloadFormatInvalid).ConfigureAwait(false);
        }

        MqttZeroConnectionContext connectionContext = new()
        {
            Client = transaction.ClientInfo,
            ConnectPacket = connectPacket
        };
        foreach (IMqttZeroConnectionInterceptor interceptor in _connectionInterceptors)
        {
            MqttZeroConnectionInterceptionResult result;
            try
            {
                result = await interceptor.InterceptAsync(
                    connectionContext,
                    CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                result = MqttZeroConnectionInterceptionResult.Reject;
                connectionContext.RejectReasonCode =
                    MqttReasonCode.ImplementationSpecificError;
            }

            if (result == MqttZeroConnectionInterceptionResult.Drop)
            {
                await transaction.NewConnection.DisposeAsync().ConfigureAwait(false);
                return false;
            }

            if (result == MqttZeroConnectionInterceptionResult.Reject)
            {
                return await RejectConnectAsync(
                    transaction,
                    connectionContext.RejectReasonCode,
                    connectionContext.ReasonString).ConfigureAwait(false);
            }
        }

        return true;
    }

    private static async ValueTask<bool> RejectConnectAsync(
        ConnectTransaction transaction,
        MqttReasonCode reasonCode,
        string? reasonString = null)
    {
        try
        {
            await transaction.NewConnection.SendConnAckAsync(
                new MqttConnAckPacket
                {
                    ReasonCode = reasonCode,
                    ReasonString = reasonString
                },
                CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            await transaction.NewConnection.DisposeAsync().ConfigureAwait(false);
        }

        return false;
    }

    private void AttachSession(ConnectTransaction transaction)
    {
        MqttConnectPacket connectPacket = transaction.ConnectPacket;
        lock (_sessionGate)
        {
            _sessions.TryGetValue(
                transaction.ClientId,
                out MqttZeroSessionState? existing);
            int expectedShardId = SessionShardMap.GetShardId(
                transaction.ClientId);
            if (existing != null &&
                existing.ShardId != expectedShardId)
            {
                throw new InvalidOperationException(
                    "The MQTT session is attached to an unexpected event loop shard.");
            }

            SelectConnectionSession(transaction, existing);
            MqttZeroSessionLifecycleSnapshot lifecycle =
                transaction.Session.Lifecycle;
            MqttZeroClientConnection? currentConnection =
                lifecycle.Connection;
            if (currentConnection != null)
            {
                transaction.OldConnection = currentConnection;
                if (lifecycle.WillMessage != null &&
                    lifecycle.WillMessage.DelayInterval == 0)
                {
                    transaction.TakeoverWill =
                        lifecycle.WillMessage;
                }
            }

            MqttZeroWillMessage? willMessage =
                connectPacket.HasWill
                    ? new MqttZeroWillMessage(connectPacket)
                    : null;
            transaction.Session.UpdateLifecycle(
                _ => lifecycle with
                {
                    Connection = transaction.NewConnection,
                    CleanStart = connectPacket.CleanStart,
                    ExpiryInterval = transaction.EffectiveExpiry,
                    DisconnectedAt = null,
                    WillDueAt = null,
                    WillMessage = willMessage
                });
            RegisterSessionDeadlines(transaction.Session);
        }
    }

    private void SelectConnectionSession(
        ConnectTransaction transaction,
        MqttZeroSessionState? existing)
    {
        MqttConnectPacket connectPacket = transaction.ConnectPacket;
        if (existing != null &&
            (connectPacket.CleanStart ||
                IsSessionExpired(existing, transaction.Now)))
        {
            transaction.ReplacedSession = existing;
            transaction.ReplacingSession = true;
            MqttZeroSessionLifecycleSnapshot lifecycle =
                existing.Lifecycle;
            transaction.OldConnection = lifecycle.Connection;
            transaction.PreviousPumpSession = existing;
            transaction.PreviousPump = existing.TakeEgress();
            if (transaction.OldConnection != null &&
                lifecycle.WillMessage != null &&
                lifecycle.WillMessage.DelayInterval == 0)
            {
                transaction.TakeoverWill = lifecycle.WillMessage;
            }

            transaction.Session = CreateSessionState(transaction.ClientId);
            transaction.SessionPresent = false;
            return;
        }

        if (existing == null)
        {
            if (_sessions.Count >= _options.MaxSessions)
            {
                throw new InvalidOperationException(
                    "The maximum number of MQTT sessions was reached.");
            }

            transaction.Session = CreateSessionState(transaction.ClientId);
            transaction.SessionPresent = false;
            return;
        }

        transaction.Session = existing;
        transaction.SessionPresent = true;
        MqttZeroSessionLifecycleSnapshot restoreLifecycle =
            existing.Lifecycle;
        transaction.PreviousCleanStart =
            restoreLifecycle.CleanStart;
        transaction.PreviousExpiryInterval =
            restoreLifecycle.ExpiryInterval;
        transaction.PreviousDisconnectedAt =
            restoreLifecycle.DisconnectedAt;
        transaction.PreviousWillMessage =
            restoreLifecycle.WillMessage;
        transaction.PreviousWillDueAt =
            restoreLifecycle.WillDueAt;
        transaction.PreviousPumpSession = existing;
        transaction.PreviousPump = existing.TakeEgress();
    }

    private async ValueTask PersistSessionAsync(
        ConnectTransaction transaction)
    {
        if (transaction.ReplacingSession)
        {
            await DeleteClientPersistenceAsync(
                transaction.ClientId,
                CancellationToken.None).ConfigureAwait(false);
            transaction.ExistingPersistenceDeleted = true;
        }

        await SaveSessionAsync(
            transaction.Session,
            CancellationToken.None).ConfigureAwait(false);
        transaction.NewSessionPersisted = true;
        lock (_sessionGate)
        {
            InstallPersistedSession(transaction);
        }
    }

    private void InstallPersistedSession(ConnectTransaction transaction)
    {
        if (transaction.ReplacedSession is { } previous)
        {
            if (!_sessions.TryGetValue(
                    transaction.ClientId,
                    out MqttZeroSessionState? current) ||
                !ReferenceEquals(current, previous))
            {
                throw new InvalidOperationException(
                    "The MQTT session changed while the connection was being established.");
            }

            previous.Connection = null;

            _sessions[transaction.ClientId] = transaction.Session;
            return;
        }

        if (transaction.SessionPresent)
        {
            return;
        }

        if (_sessions.Count >= _options.MaxSessions)
        {
            throw new InvalidOperationException(
                "The maximum number of MQTT sessions was reached.");
        }

        _sessions[transaction.ClientId] = transaction.Session;
    }

    private async ValueTask ActivateConnectionAsync(
        ConnectTransaction transaction)
    {
        await StopEgressAsync(transaction.PreviousPump).ConfigureAwait(false);
        transaction.PreviousPumpStopped = transaction.PreviousPump != null;
        await PublishTakeoverWillAsync(transaction).ConfigureAwait(false);
        await SendSuccessfulConnAckAsync(transaction).ConfigureAwait(false);
        transaction.NewConnection.IsConnectedClientCounted = true;
        Interlocked.Increment(ref _connectedClientCount);
        await ClosePreviousConnectionAsync(transaction).ConfigureAwait(false);

        if (transaction.ReplacedSession != null)
        {
            ReleaseSessionMessagesCore(transaction.ReplacedSession);
        }

        MqttZeroConnectedClient connectedClient = new(
            this,
            transaction.Session,
            transaction.NewConnection);
        transaction.NewConnection.ConnectedClient = connectedClient;
        await NotifyConnectedAsync(
            connectedClient,
            transaction.SessionPresent).ConfigureAwait(false);

        if (transaction.ReplacingSession &&
            transaction.ReplacedSession != null)
        {
            await RemoveSessionSubscriptionsFromRouteAsync(
                transaction.ReplacedSession).ConfigureAwait(false);
        }
    }

    private async ValueTask PublishTakeoverWillAsync(
        ConnectTransaction transaction)
    {
        if (transaction.TakeoverWill == null)
        {
            return;
        }

        await RoutePublishAsync(
            null,
            transaction.TakeoverWill.CreatePacket(),
            CancellationToken.None).ConfigureAwait(false);
        Interlocked.Increment(ref _publishedWillCount);
    }

    private async ValueTask SendSuccessfulConnAckAsync(
        ConnectTransaction transaction)
    {
        await transaction.NewConnection.SendConnAckAsync(
            new MqttConnAckPacket
            {
                SessionPresent = transaction.SessionPresent,
                ReasonCode = MqttReasonCode.Success,
                TopicAliasMaximum = _options.TopicAliasMaximum,
                ReceiveMaximum =
                    (ushort)Math.Min(_options.MaxInflight, ushort.MaxValue),
                MaximumPacketSize = (uint)_options.MaxPacketSize,
                SessionExpiryInterval = transaction.EffectiveExpiry,
                AssignedClientIdentifier =
                    transaction.AssignedClientIdentifier
                        ? transaction.ClientId
                        : null,
                ResponseInformation =
                    transaction.ConnectPacket.RequestResponseInformation
                        ? _options.ResponseInformation
                        : null
            },
            CancellationToken.None).ConfigureAwait(false);
    }

    private async ValueTask ClosePreviousConnectionAsync(
        ConnectTransaction transaction)
    {
        if (transaction.OldConnection == null ||
            ReferenceEquals(
                transaction.OldConnection,
                transaction.NewConnection))
        {
            return;
        }

        try
        {
            await transaction.OldConnection.CloseAsync(
                MqttReasonCode.SessionTakenOver,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogConnectionCleanupFailure(
                _logger,
                exception);
        }
    }

    private async ValueTask NotifyConnectedAsync(
        MqttZeroConnectedClient connectedClient,
        bool sessionPresent)
    {
        MqttZeroClientConnectedContext connectedContext = new()
        {
            ConnectedClient = connectedClient,
            SessionPresent = sessionPresent
        };
        foreach (IMqttZeroConnectionObserver observer in _connectionObservers)
        {
            try
            {
                await observer.OnConnectedAsync(
                    connectedContext,
                    CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
            }
        }
    }

    private async ValueTask RollbackTransactionAsync(
        ConnectTransaction transaction,
        bool registerDeadlines,
        bool removeDeadlines,
        bool persistRestoredSession)
    {
        await RollbackConnectionAsync(
            transaction.ClientId,
            transaction.Session,
            transaction.SessionPresent,
            transaction.NewSessionPersisted,
            transaction.ExistingPersistenceDeleted,
            CreateRestoreState(
                transaction.ReplacedSession,
                transaction.OldConnection,
                transaction.PreviousPumpSession,
                transaction.PreviousPump,
                transaction.PreviousPumpStopped,
                transaction.PreviousCleanStart,
                transaction.PreviousExpiryInterval,
                transaction.PreviousDisconnectedAt,
                transaction.PreviousWillMessage,
                transaction.PreviousWillDueAt),
            registerDeadlines,
            removeDeadlines,
            persistRestoredSession).ConfigureAwait(false);
    }

    private static void CancelAcceptedConnection(object? state)
    {
        if (state is CancellationTokenSource source)
        {
            source.Cancel();
        }
    }

    private static MqttZeroSessionRestoreState CreateRestoreState(
        MqttZeroSessionState? replacedSession,
        MqttZeroClientConnection? oldConnection,
        MqttZeroSessionState? previousPumpSession,
        MqttZeroSessionEgress? previousPump,
        bool previousPumpStopped,
        bool previousCleanStart,
        uint previousExpiryInterval,
        DateTimeOffset? previousDisconnectedAt,
        MqttZeroWillMessage? previousWillMessage,
        DateTimeOffset? previousWillDueAt)
    {
        return new MqttZeroSessionRestoreState(
            replacedSession,
            oldConnection,
            previousPumpSession,
            previousPump,
            previousPumpStopped,
            previousCleanStart,
            previousExpiryInterval,
            previousDisconnectedAt,
            previousWillMessage,
            previousWillDueAt);
    }

    private async ValueTask RollbackConnectionAsync(
        string clientId,
        MqttZeroSessionState session,
        bool sessionPresent,
        bool newSessionPersisted,
        bool existingPersistenceDeleted,
        MqttZeroSessionRestoreState restore,
        bool registerDeadlines,
        bool removeDeadlines,
        bool persistRestoredSession)
    {
        if (restore.ReplacedSession is { } replacedSession)
        {
            await RollbackReplacedSessionAsync(
                clientId,
                session,
                existingPersistenceDeleted,
                replacedSession,
                restore,
                registerDeadlines).ConfigureAwait(false);
            return;
        }

        if (!sessionPresent)
        {
            await RollbackNewSessionAsync(
                clientId,
                session,
                newSessionPersisted,
                removeDeadlines).ConfigureAwait(false);
            return;
        }

        await RollbackExistingSessionAsync(
            clientId,
            session,
            restore,
            persistRestoredSession).ConfigureAwait(false);
    }

    private async ValueTask RollbackReplacedSessionAsync(
        string clientId,
        MqttZeroSessionState session,
        bool existingPersistenceDeleted,
        MqttZeroSessionState replacedSession,
        MqttZeroSessionRestoreState restore,
        bool registerDeadlines)
    {
        lock (_sessionGate)
        {
            if (_sessions.TryGetValue(
                    clientId,
                    out MqttZeroSessionState? current) &&
                ReferenceEquals(current, session))
            {
                _sessions[clientId] = replacedSession;
            }
        }

        replacedSession.UpdateLifecycle(
            current => current with
            {
                Connection = restore.OldConnection
            });

        if (registerDeadlines)
        {
            RegisterSessionDeadlines(replacedSession);
        }

        RestoreSessionEgress(
            restore.PreviousPumpSession,
            restore.OldConnection,
            restore.PreviousPump,
            restore.PreviousPumpStopped);
        if (!existingPersistenceDeleted)
        {
            return;
        }

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

    private async ValueTask RollbackNewSessionAsync(
        string clientId,
        MqttZeroSessionState session,
        bool newSessionPersisted,
        bool removeDeadlines)
    {
        lock (_sessionGate)
        {
            _sessions.TryRemove(session.ClientId, out _);
        }

        if (removeDeadlines)
        {
            RemoveSessionDeadlines(session);
        }

        ReleaseSessionMessagesCore(session);
        if (!newSessionPersisted)
        {
            return;
        }

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

    private async ValueTask RollbackExistingSessionAsync(
        string clientId,
        MqttZeroSessionState session,
        MqttZeroSessionRestoreState restore,
        bool persistRestoredSession)
    {
        session.UpdateLifecycle(
            current => current with
            {
                Connection = restore.OldConnection,
                CleanStart = restore.PreviousCleanStart,
                ExpiryInterval = restore.PreviousExpiryInterval,
                DisconnectedAt = restore.PreviousDisconnectedAt,
                WillMessage = restore.PreviousWillMessage,
                WillDueAt = restore.PreviousWillDueAt
            });

        RestoreSessionEgress(
            restore.PreviousPumpSession,
            restore.OldConnection,
            restore.PreviousPump,
            restore.PreviousPumpStopped);
        if (!persistRestoredSession)
        {
            return;
        }

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

    private void RestoreSessionEgress(
        MqttZeroSessionState? session,
        MqttZeroClientConnection? connection,
        MqttZeroSessionEgress? pump,
        bool pumpStopped)
    {
        if (session == null || connection == null)
        {
            return;
        }

        if (!pumpStopped && pump != null)
        {
            if (!session.TryAttachEgress(connection, pump))
            {
                return;
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

    private async ValueTask OwnInboundLeaseAsync(
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                lease.EnsureOwned();
                return;
            }
            catch (MqttZeroBufferPoolExhaustedException exhaustion)
            {
                await _bufferPool.WaitForCapacityAsync(
                    exhaustion.ReturnGeneration,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

}
