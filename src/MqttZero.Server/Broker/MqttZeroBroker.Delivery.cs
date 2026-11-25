using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Server.Observers;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements delivery, queues, inflight messages, and session drain.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private bool UseFastFanoutDelivery
    {
        get
        {
            return !_sessionStore.IsEnabled &&
                !_messageStore.IsEnabled &&
                !_retainedMessageStore.IsEnabled &&
                _enqueueInterceptors.Length == 0 &&
                _deliveryObservers.Length == 0;
        }
    }

    private ValueTask DeliverPlannedAsync(
        MqttZeroPlannedDelivery delivery,
        MqttZeroMessageLease lease,
        MqttZeroClientConnection? senderConnection,
        CancellationToken cancellationToken)
    {
        MqttQualityOfServiceLevel deliveryQos =
            (MqttQualityOfServiceLevel)Math.Min(
                (byte)lease.QualityOfServiceLevel,
                (byte)delivery.QualityOfServiceLevel);
        bool retain =
            delivery.RetainAsPublished && lease.RetainFlag;
        ReadOnlyMemory<uint> subscriptionIdentifiers =
            delivery.SubscriptionIdentifiersMemory;
        if (UseFastFanoutDelivery)
        {
            return DeliverLeaseFastAsync(
                delivery.Session,
                lease,
                deliveryQos,
                retain,
                subscriptionIdentifiers,
                cancellationToken,
                copySubscriptionIdentifiers: true);
        }

        return DeliverLeaseAsync(
            delivery.Session,
            lease,
            deliveryQos,
            retain,
            subscriptionIdentifiers,
            senderConnection,
            cancellationToken,
            copySubscriptionIdentifiers: true);
    }

    private void HandleFanoutFailure(Exception exception)
    {
        MqttZeroBrokerLogging.LogFanoutFailure(
            _logger,
            exception);
        _fanoutDiagnostics.RecordFanoutFailure();
        if (Interlocked.Exchange(ref _fanoutFailed, 1) == 0)
        {
            _ = StopAfterFanoutFailureAsync();
        }
    }

    private async Task StopAfterFanoutFailureAsync()
    {
        try
        {
            await StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogFanoutFailure(
                _logger,
                exception);
        }
    }

    private ValueTask<int> RouteToSubscribersAsync(
        MqttZeroSessionState? sender,
        MqttZeroClientConnection? senderConnection,
        MqttZeroMessageLease lease,
        bool scheduleWithoutWaiting,
        CancellationToken cancellationToken)
    {
        lease.EnsureOwned();
        MqttZeroShardSet shards = Volatile.Read(ref _shardSet) ??
            throw new InvalidOperationException(
                "The MQTTZero event loop shards are not running.");
        MqttZeroFanoutDispatcher dispatcher =
            Volatile.Read(ref _fanoutDispatcher) ??
            throw new InvalidOperationException(
                "The MQTTZero fanout dispatcher is not running.");
        MqttZeroRoutePlannerLanes lanes =
            Volatile.Read(ref _routePlannerLanes) ??
            throw new InvalidOperationException(
                "The MQTTZero route planner is not running.");
        int publisherShardId = sender?.ShardId ??
            senderConnection?.ShardId ??
            0;
        if ((uint)publisherShardId >= (uint)shards.Count)
        {
            throw new InvalidOperationException(
                "The publisher references an invalid event loop shard.");
        }

        MqttZeroEventLoop publisherShard =
            shards[publisherShardId];
        if (publisherShard.IsCurrent)
        {
            return ExecuteFanoutOnShardAsync(
                shards,
                dispatcher,
                lanes,
                sender,
                senderConnection,
                lease,
                publisherShardId,
                scheduleWithoutWaiting,
                cancellationToken);
        }

        return publisherShard.InvokeAsync(
            token => ExecuteFanoutOnShardAsync(
                shards,
                dispatcher,
                lanes,
                sender,
                senderConnection,
                lease,
                publisherShardId,
                scheduleWithoutWaiting,
                token),
            cancellationToken);
    }

    private async ValueTask<int> ExecuteFanoutOnShardAsync(
        MqttZeroShardSet shards,
        MqttZeroFanoutDispatcher dispatcher,
        MqttZeroRoutePlannerLanes lanes,
        MqttZeroSessionState? sender,
        MqttZeroClientConnection? senderConnection,
        MqttZeroMessageLease lease,
        int publisherShardId,
        bool scheduleWithoutWaiting,
        CancellationToken cancellationToken)
    {
        MqttZeroRoutePlanBuffer buffer = lanes[publisherShardId];
        MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
            _routeEpochs.Current,
            lease.Topic.Span,
            sender,
            _sharedGroupCursors,
            shards.Count,
            buffer);
        int deliveryCount = plan.TotalDeliveryCount;
        if (deliveryCount > 0)
        {
            if (scheduleWithoutWaiting)
            {
                await dispatcher.DispatchScheduledAsync(
                    plan,
                    buffer,
                    lease,
                    senderConnection,
                    publisherShardId,
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await dispatcher.DispatchAsync(
                    plan,
                    buffer,
                    lease,
                    senderConnection,
                    publisherShardId,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        return deliveryCount;
    }

    private async ValueTask DeliverAsync(
        MqttZeroSessionState session,
        MqttPublishPacket source,
        MqttQualityOfServiceLevel qos,
        bool retain,
        uint? subscriptionIdentifier,
        CancellationToken cancellationToken)
    {
        await DeliverAsync(
            session,
            source,
            qos,
            retain,
            subscriptionIdentifier.HasValue
                ? new[] { subscriptionIdentifier.Value }
                : ReadOnlyMemory<uint>.Empty,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask DeliverAsync(
        MqttZeroSessionState session,
        MqttPublishPacket source,
        MqttQualityOfServiceLevel qos,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        CancellationToken cancellationToken)
    {
        ReadOnlySequence<byte> properties = MqttPublishPropertiesCodec.Encode(
            source.Properties);
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            source.TopicName,
            properties,
            source.Payload,
            qos,
            retain,
            source.Properties.MessageExpiryInterval,
            null,
            _bufferPool);
        lease.SetExpiresAt(CalculateMessageExpiresAt(source.Properties.MessageExpiryInterval));
        try
        {
            await DeliverLeaseAsync(
                session,
                lease,
                qos,
                retain,
                subscriptionIdentifiers,
                null,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            lease.Release();
        }
    }

    private async ValueTask DeliverLeaseAsync(
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qos,
        bool retain,
        uint? subscriptionIdentifier,
        MqttZeroClientConnection? senderConnection,
        CancellationToken cancellationToken)
    {
        await DeliverLeaseAsync(
            session,
            lease,
            qos,
            retain,
            subscriptionIdentifier.HasValue
                ? new[] { subscriptionIdentifier.Value }
                : ReadOnlyMemory<uint>.Empty,
            senderConnection,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask DeliverLeaseAsync(
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qos,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        MqttZeroClientConnection? senderConnection,
        CancellationToken cancellationToken,
        bool copySubscriptionIdentifiers = false)
    {
        DateTimeOffset? expiresAt = lease.ExpiresAt;
        if (expiresAt.HasValue &&
            expiresAt.Value <= _timeProvider.GetUtcNow())
        {
            await NotifyDroppedAsync(
                senderConnection?.ConnectedClient,
                session,
                lease,
                MqttZeroMessageDropReason.Expired,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        MqttZeroEnqueueContext? enqueueContext = null;
        if (_enqueueInterceptors.Length > 0)
        {
            enqueueContext = new MqttZeroEnqueueContext
            {
                Sender = senderConnection?.ConnectedClient,
                ReceiverInfo = session.Connection?.ClientInfo ??
                    CreateOfflineClientInfo(session),
                Receiver = session.Connection?.ConnectedClient,
                Topic = new ReadOnlySequence<byte>(lease.Topic),
                PayloadLength = lease.PayloadSequence.Length,
                QualityOfServiceLevel = qos,
                Retain = retain,
                SubscriptionIdentifiers = subscriptionIdentifiers
            };
            foreach (IMqttZeroEnqueueInterceptor interceptor in _enqueueInterceptors)
            {
                MqttZeroEnqueueInterceptionResult result =
                    await interceptor.InterceptAsync(enqueueContext, cancellationToken).ConfigureAwait(false);
                if (result == MqttZeroEnqueueInterceptionResult.Drop)
                {
                    await NotifyDroppedAsync(
                        senderConnection?.ConnectedClient,
                        session,
                        lease,
                        MqttZeroMessageDropReason.InterceptorRejected,
                        cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (result == MqttZeroEnqueueInterceptionResult.CloseSenderConnection)
                {
                    if (senderConnection != null)
                    {
                        await senderConnection.CloseAsync(
                            MqttReasonCode.NotAuthorized,
                            cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }
            }
        }

        MqttZeroQueuedMessage? message = null;
        MqttZeroClientConnection? connection = null;
        bool messageHeld = false;
        MqttZeroQueuedMessage? evicted = null;
        bool dropOfflineQos0 = false;
        bool packetTooLarge = false;
        int encodedLength = 0;

        try
        {
            lock (session.SyncRoot)
            {
                connection = session.Connection;
                if (connection != null)
                {
                    encodedLength = connection.GetPublishEncodedLength(
                        lease,
                        qos,
                        retain,
                        subscriptionIdentifiers);
                    if (encodedLength > connection.MaximumPacketSize)
                    {
                        packetTooLarge = true;
                    }
                }

                if (!packetTooLarge)
                {
                    if (qos == MqttQualityOfServiceLevel.AtMostOnce &&
                        connection == null &&
                        !_options.StoreQos0InQueue)
                    {
                        dropOfflineQos0 = true;
                    }
                    else
                    {
                        message = copySubscriptionIdentifiers
                            ? MqttZeroQueuedMessage.RentWithCopiedIdentifiers(
                                lease,
                                qos,
                                retain,
                                subscriptionIdentifiers,
                                expiresAt)
                            : MqttZeroQueuedMessage.RentWithIdentifiers(
                                lease,
                                qos,
                                retain,
                                subscriptionIdentifiers,
                                expiresAt);
                        if (connection != null)
                        {
                            message.SetEncodedLength(
                                encodedLength,
                                connection.MaximumPacketSize);
                        }

                        message.SetMessageId(GetNextMessageId());
                        evicted = EnqueueMessage(session, message);
                        messageHeld = true;
                    }
                }
            }

            if (packetTooLarge)
            {
                await NotifyDroppedAsync(
                    senderConnection?.ConnectedClient,
                    session,
                    lease,
                    MqttZeroMessageDropReason.PacketTooLarge,
                    cancellationToken).ConfigureAwait(false);
                return;
            }

            if (dropOfflineQos0)
            {
                await NotifyDroppedAsync(
                    senderConnection?.ConnectedClient,
                    session,
                    lease,
                    MqttZeroMessageDropReason.OfflineClient,
                    cancellationToken).ConfigureAwait(false);
                return;
            }

            if (evicted != null)
            {
                await NotifyOverwrittenAsync(
                    session,
                    session.Connection?.ClientInfo ?? CreateOfflineClientInfo(session),
                    evicted,
                    cancellationToken).ConfigureAwait(false);
            }

            if (messageHeld && _messageStore.IsEnabled)
            {
                try
                {
                    if (evicted != null)
                    {
                        await ReplaceQueuedAsync(
                            session.ClientId,
                            evicted.MessageId,
                            message!,
                            cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await SaveQueuedAsync(
                            message!,
                            session.ClientId,
                            cancellationToken).ConfigureAwait(false);
                    }
                }
                catch
                {
                    lock (session.SyncRoot)
                    {
                        int index = session.PendingMessages.FindIndex(
                            item => ReferenceEquals(item, message));
                        if (index >= 0)
                        {
                            session.PendingMessages.RemoveAt(index);
                        }

                        if (evicted != null)
                        {
                            session.PendingMessages.Prepend(evicted);
                        }
                    }

                    message!.Release();
                    messageHeld = false;
                    if (connection != null)
                    {
                        await connection.CloseAsync(
                            MqttReasonCode.UnspecifiedError,
                            cancellationToken).ConfigureAwait(false);
                    }

                    throw;
                }
            }

            if (evicted != null)
            {
                evicted.Release();
            }

            if (messageHeld && _deliveryObservers.Length > 0)
            {
                MqttZeroClientConnectionInfo receiver = enqueueContext?.ReceiverInfo ??
                    session.Connection?.ClientInfo ??
                    CreateOfflineClientInfo(session);
                MqttZeroMessageEnqueuedContext observerContext = new()
                {
                    Sender = senderConnection?.ConnectedClient,
                    ReceiverInfo = receiver,
                    Receiver = session.Connection?.ConnectedClient,
                    Topic = new ReadOnlySequence<byte>(lease.Topic),
                    PayloadLength = lease.PayloadSequence.Length,
                    QualityOfServiceLevel = qos
                };
                await NotifyEnqueuedAsync(observerContext, cancellationToken).ConfigureAwait(false);
            }

            if (messageHeld)
            {
                SignalEgress(session);
            }
        }
        finally
        {
            if (message != null && !messageHeld)
            {
                message.Release();
            }
        }
    }

    private MqttZeroQueuedMessage? EnqueueMessage(
        MqttZeroSessionState session,
        MqttZeroQueuedMessage message)
    {
        MqttZeroQueuedMessage? evicted = null;
        if (session.PendingMessages.Count >= _options.MaxMessageQueueLength)
        {
            evicted = session.PendingMessages.EvictOldestQos0OrOldest();
        }

        session.PendingMessages.Add(message);
        _fanoutDiagnostics.RecordPendingQueueDepth(
            session.PendingMessages.Count);
        return evicted;
    }

    private void SignalEgress(MqttZeroSessionState session)
    {
        MqttZeroEgressActor? egress = session.Egress;
        if (egress == null)
        {
            return;
        }

        _fanoutDiagnostics.RecordEgressSignal();
        egress.Signal();
    }

    private static MqttZeroClientConnectionInfo CreateOfflineClientInfo(
        MqttZeroSessionState session)
    {
        return new MqttZeroClientConnectionInfo
        {
            ClientId = session.ClientId
        };
    }

    private async ValueTask NotifyNotConsumedAsync(
        MqttZeroConnectedClient? sender,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        if (_deliveryObservers.Length == 0)
        {
            return;
        }

        MqttZeroMessageNotConsumedContext context = new()
        {
            Sender = sender,
            Topic = new ReadOnlySequence<byte>(lease.Topic),
            PayloadLength = lease.PayloadSequence.Length
        };
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                await observer.OnMessageNotConsumedAsync(context, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
            }
        }
    }

    private async ValueTask NotifyDroppedAsync(
        MqttZeroConnectedClient? sender,
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttZeroMessageDropReason reason,
        CancellationToken cancellationToken)
    {
        if (_deliveryObservers.Length == 0)
        {
            return;
        }

        MqttZeroMessageDroppedContext context = new()
        {
            Sender = sender,
            ReceiverInfo = session.Connection?.ClientInfo,
            Receiver = session.Connection?.ConnectedClient,
            Topic = new ReadOnlySequence<byte>(lease.Topic),
            Reason = reason
        };
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                await observer.OnMessageDroppedAsync(context, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
            }
        }
    }

    private async ValueTask NotifyEnqueuedAsync(
        MqttZeroMessageEnqueuedContext context,
        CancellationToken cancellationToken)
    {
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                await observer.OnMessageEnqueuedAsync(context, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
            }
        }
    }

    private async ValueTask ProcessOutboundPubRecAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        MqttZeroQueuedMessage? message;
        lock (session.SyncRoot)
        {
            session.InflightMessages.TryGetValue(packetIdentifier, out message);
        }

        if (message == null ||
            message.QualityOfServiceLevel != MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return;
        }

        if (reasonCode >= MqttReasonCode.UnspecifiedError)
        {
            MqttZeroQueuedMessage? failed = null;
            lock (session.SyncRoot)
            {
                session.InflightMessages.Remove(packetIdentifier, out failed);
            }

            if (failed != null)
            {
                try
                {
                    await DeleteMessageAsync(
                        session.ClientId,
                        failed.MessageId,
                        cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    lock (session.SyncRoot)
                    {
                        session.InflightMessages[packetIdentifier] = failed;
                    }

                    throw;
                }

                failed.Release();
            }

            SignalEgress(session);
            return;
        }

        message.SetAwaitPubComp();
        try
        {
            await SaveOutboundAsync(
                message,
                session.ClientId,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            message.SetAwaitPubRec();
            throw;
        }

        await connection.SendPubRelAsync(
            packetIdentifier,
            MqttReasonCode.Success,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ProcessOutboundPubCompAsync(
        MqttZeroSessionState session,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        MqttZeroQueuedMessage? completed = null;
        lock (session.SyncRoot)
        {
            session.InflightMessages.Remove(packetIdentifier, out completed);
        }

        if (completed != null)
        {
            try
            {
                await DeleteMessageAsync(
                    session.ClientId,
                    completed.MessageId,
                    cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                lock (session.SyncRoot)
                {
                    session.InflightMessages[packetIdentifier] = completed;
                }

                throw;
            }

            completed.Release();
        }

        SignalEgress(session);
    }

    private async ValueTask ProcessInboundPubRelAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        MqttZeroMessageLease? lease;
        MqttReasonCode completionReason = MqttReasonCode.Success;
        bool routeCompleted;
        bool stateRemoved = false;
        lock (session.SyncRoot)
        {
            session.InboundQos2Messages.TryGetValue(packetIdentifier, out lease);
            routeCompleted = session.RoutedInboundQos2Messages.TryGetValue(
                packetIdentifier,
                out completionReason);
        }

        if (lease == null)
        {
            await connection.SendPubCompAsync(
                packetIdentifier,
                MqttReasonCode.PacketIdentifierNotFound,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        if (!routeCompleted && CanDispatchInboundQos2)
        {
            await EnqueueInboundAsync(
                lease,
                session,
                connection,
                cancellationToken).ConfigureAwait(false);
            lock (session.SyncRoot)
            {
                session.InboundQos2Messages.Remove(packetIdentifier);
                session.RoutedInboundQos2Messages.Remove(packetIdentifier);
            }

            lease.Release();
            await connection.SendPubCompAsync(
                packetIdentifier,
                MqttReasonCode.Success,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            if (!routeCompleted)
            {
                await RoutePreparedLeaseAsync(
                    session,
                    connection,
                    lease,
                    cancellationToken).ConfigureAwait(false);
                completionReason = MqttReasonCode.Success;
                lock (session.SyncRoot)
                {
                    session.RoutedInboundQos2Messages[packetIdentifier] =
                        completionReason;
                }
            }

            await DeleteInboundQos2Async(
                session.ClientId,
                packetIdentifier,
                cancellationToken).ConfigureAwait(false);
            lock (session.SyncRoot)
            {
                session.InboundQos2Messages.Remove(packetIdentifier);
                session.RoutedInboundQos2Messages.Remove(packetIdentifier);
                stateRemoved = true;
            }

            await connection.SendPubCompAsync(
                packetIdentifier,
                completionReason,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (stateRemoved)
            {
                lease.Release();
            }
        }
    }

    private async ValueTask NotifyOverwrittenAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnectionInfo receiver,
        MqttZeroQueuedMessage evicted,
        CancellationToken cancellationToken)
    {
        MqttZeroQueueMessageOverwrittenContext context = new()
        {
            ReceiverInfo = receiver,
            Receiver = session.Connection?.ConnectedClient,
            Topic = new ReadOnlySequence<byte>(evicted.Topic)
        };
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                await observer.OnQueueMessageOverwrittenAsync(
                    context,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
            }
        }
    }

    internal MqttZeroEgressActor StartEgress(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        CancellationToken cancellationToken)
    {
        int resumePending = 1;
        MqttZeroEgressActor? egress = null;

        async ValueTask<bool> DrainAsync(
            CancellationToken pumpCancellationToken)
        {
            long started = Stopwatch.GetTimestamp();
            try
            {
                if (Interlocked.Exchange(ref resumePending, 0) != 0)
                {
                    await ResumeInflightMessagesAsync(
                        session,
                        connection,
                        pumpCancellationToken).ConfigureAwait(false);
                }

                return await DrainQueuedMessagesOnceAsync(
                    session,
                    connection,
                    egress!,
                    pumpCancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _fanoutDiagnostics.RecordEgressDrainTiming(
                    Stopwatch.GetTimestamp() - started);
            }
        }

        egress = new MqttZeroEgressActor(
            _shardSet!.GetEgressScheduler(session.ShardId),
            connection.WriteEncodedControlBatchAsync,
            DrainAsync,
            exception =>
            {
                MqttZeroBrokerLogging.LogEgressFailure(
                    _logger,
                    session.ClientId,
                    exception);
                lock (session.SyncRoot)
                {
                    if (ReferenceEquals(session.Egress, egress))
                    {
                        session.Egress = null;
                    }
                }

                connection.Abort();
            });
        lock (session.SyncRoot)
        {
            if (session.Egress != null)
            {
                throw new InvalidOperationException(
                    "The MQTT session already has an outbound pump.");
            }

            session.Egress = egress;
        }

        connection.AttachEgress(egress);
        egress.Signal();
        return egress;
    }

    private static async ValueTask ResumeInflightMessagesAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        CancellationToken cancellationToken)
    {
        List<MqttZeroQueuedMessage> inflight;
        lock (session.SyncRoot)
        {
            inflight = [.. session.InflightMessages.Values];
        }

        foreach (MqttZeroQueuedMessage message in inflight)
        {
            if (message.State == MqttZeroOutboundState.AwaitPubComp)
            {
                await connection.SendPubRelAsync(
                    message.PacketIdentifier,
                    MqttReasonCode.Success,
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await connection.SendPublishAsync(message, true, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<bool> DrainQueuedMessagesOnceAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroEgressActor egress,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            long batchStartedTimestamp = _timeProvider.GetTimestamp();
            long batchDurationTicks = Math.Max(
                1,
                (long)(
                    _options.OutboundBatchDuration.TotalSeconds *
                    _timeProvider.TimestampFrequency));
            MqttZeroEgressBatchBudget budget = new(
                _options.OutboundBatchBytes,
                _options.OutboundBatchMessages,
                batchStartedTimestamp + batchDurationTicks);
            MqttZeroQueuedMessage? next = null;
            bool hasNext = false;
            bool nextTooLarge = false;
            List<MqttZeroQueuedMessage> expired = session.ExpiredMessages;
            List<MqttZeroQueuedMessage> batch = session.OutboundBatch;
            lock (session.SyncRoot)
            {
                expired.Clear();
                batch.Clear();
                if (session.PendingMessages.HasExpiringMessages)
                {
                    DateTimeOffset now = _timeProvider.GetUtcNow();
                    session.PendingMessages.RemoveExpired(now, expired);
                }
                hasNext = session.PendingMessages.Count > 0 &&
                    (session.PendingMessages[0].QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.AtMostOnce ||
                    session.InflightMessages.Count < GetInflightLimit(connection));
                if (hasNext)
                {
                    next = session.PendingMessages.Dequeue();
                    ValidateEncodedLength(
                        connection,
                        next,
                        out nextTooLarge);
                    if (!nextTooLarge)
                    {
                        _ = budget.TryAdd(
                            next.EncodedLength,
                            _timeProvider.GetTimestamp());
                        batch.Add(next);
                        if (next.QualityOfServiceLevel !=
                            MqttQualityOfServiceLevel.AtMostOnce)
                        {
                            next.PacketIdentifier = session.GetNextPacketIdentifier();
                            session.InflightMessages[next.PacketIdentifier] = next;
                        }

                        while (!connection.HasPendingControlWrites &&
                            session.PendingMessages.Count > 0)
                        {
                            MqttZeroQueuedMessage head =
                                session.PendingMessages[0];
                            if (head.QualityOfServiceLevel !=
                                    MqttQualityOfServiceLevel.AtMostOnce &&
                                session.InflightMessages.Count >=
                                    GetInflightLimit(connection))
                            {
                                break;
                            }

                            ValidateEncodedLength(
                                connection,
                                head,
                                out bool headTooLarge);
                            if (headTooLarge ||
                                !budget.TryAdd(
                                    head.EncodedLength,
                                    _timeProvider.GetTimestamp()))
                            {
                                break;
                            }

                            MqttZeroQueuedMessage candidate =
                                session.PendingMessages.Dequeue();
                            batch.Add(candidate);
                            if (candidate.QualityOfServiceLevel !=
                                MqttQualityOfServiceLevel.AtMostOnce)
                            {
                                candidate.PacketIdentifier =
                                    session.GetNextPacketIdentifier();
                                session.InflightMessages[candidate.PacketIdentifier] =
                                    candidate;
                            }

                        }
                    }
                }
            }

            for (int index = 0; index < expired.Count; index++)
            {
                MqttZeroQueuedMessage expiredMessage = expired[index];
                try
                {
                    await DeleteMessageAsync(
                        session.ClientId,
                        expiredMessage.MessageId,
                        cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    lock (session.SyncRoot)
                    {
                        for (int restore = expired.Count - 1; restore >= index; restore--)
                        {
                            session.PendingMessages.Prepend(expired[restore]);
                        }
                    }

                    throw;
                }

                await NotifyDroppedAsync(
                    null,
                    session,
                    expiredMessage.Lease,
                    MqttZeroMessageDropReason.Expired,
                    cancellationToken).ConfigureAwait(false);
                expiredMessage.Release();
            }

            expired.Clear();
            if (nextTooLarge)
            {
                try
                {
                    await DeleteMessageAsync(
                        session.ClientId,
                        next!.MessageId,
                        cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    lock (session.SyncRoot)
                    {
                        session.PendingMessages.Prepend(next!);
                    }

                    throw;
                }

                await NotifyDroppedAsync(
                    null,
                    session,
                    next!.Lease,
                    MqttZeroMessageDropReason.PacketTooLarge,
                    cancellationToken).ConfigureAwait(false);
                next.Release();
                continue;
            }

            if (!hasNext)
            {
                return false;
            }

            if (batch.Count > 0)
            {
                for (int index = 0; index < batch.Count; index++)
                {
                    MqttZeroQueuedMessage message = batch[index];
                    if (message.QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.AtMostOnce)
                    {
                        continue;
                    }

                    try
                    {
                        await TransitionQueuedToOutboundAsync(
                            message,
                            session.ClientId,
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        lock (session.SyncRoot)
                        {
                            for (int restore = batch.Count - 1;
                                restore >= index;
                                restore--)
                            {
                                MqttZeroQueuedMessage restoreMessage =
                                    batch[restore];
                                if (restoreMessage.QualityOfServiceLevel !=
                                    MqttQualityOfServiceLevel.AtMostOnce)
                                {
                                    session.InflightMessages.Remove(
                                        restoreMessage.PacketIdentifier);
                                }

                                session.PendingMessages.Prepend(restoreMessage);
                            }
                        }

                        batch.Clear();
                        throw;
                    }
                }

                try
                {
                    _fanoutDiagnostics.RecordEgressDataBatch(
                        batch.Count,
                        budget.EncodedBytes);
                    _fanoutDiagnostics.RecordEgressFlushStart();
                    if (!connection.TryEncodePublishBatch(
                            batch,
                            out _))
                    {
                        await connection.SendPublishBatchAsync(
                            batch,
                            cancellationToken).ConfigureAwait(false);
                    }

                    _fanoutDiagnostics.RecordEncodedPublishes(
                        batch.Count);
                    for (int index = 0; index < batch.Count; index++)
                    {
                        MqttZeroQueuedMessage message = batch[index];
                        if (message.QualityOfServiceLevel ==
                            MqttQualityOfServiceLevel.AtMostOnce)
                        {
                            message.Release();
                        }
                    }

                    if (connection.AllowPacketFragmentation &&
                        !egress.BeginFlush(
                            connection.FlushOutputAsync(
                                cancellationToken)))
                    {
                        _fanoutDiagnostics.RecordFlushWait();
                        batch.Clear();
                        return session.PendingMessages.Count > 0;
                    }
                }
                catch
                {
                    for (int index = 0; index < batch.Count; index++)
                    {
                        MqttZeroQueuedMessage message = batch[index];
                        if (message.QualityOfServiceLevel ==
                            MqttQualityOfServiceLevel.AtMostOnce)
                        {
                            message.Release();
                        }
                    }

                    batch.Clear();
                    throw;
                }

                batch.Clear();
                return session.PendingMessages.Count > 0;
            }
        }
    }

    private static void ValidateEncodedLength(
        MqttZeroClientConnection connection,
        MqttZeroQueuedMessage message,
        out bool tooLarge)
    {
        if (!message.IsEncodedLengthValidFor(connection.MaximumPacketSize))
        {
            int encodedLength = connection.GetPublishEncodedLength(
                message.Lease,
                message.QualityOfServiceLevel,
                message.Retain,
                message.SubscriptionIdentifiers);
            message.SetEncodedLength(
                encodedLength,
                connection.MaximumPacketSize);
        }

        tooLarge = message.EncodedLength > connection.MaximumPacketSize;
    }

    internal bool CanDispatchInbound
    {
        get
        {
            return _shardSet != null;
        }
    }

    private bool CanDispatchInboundQos2
    {
        get
        {
            return CanDispatchInbound && !_messageStore.IsEnabled;
        }
    }

    internal async ValueTask EnqueueInboundAsync(
        MqttZeroMessageLease lease,
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        CancellationToken cancellationToken,
        long messageId = 0)
    {
        MqttZeroShardSet shards = _shardSet ??
            throw new InvalidOperationException(
                "The event loop shards are not running.");
        MqttZeroInboundWork work = MqttZeroInboundWork.Rent(
            lease,
            sender,
            senderConnection,
            messageId);
        MqttZeroInboundShardWork shardWork = MqttZeroInboundShardWork.Rent(
            work,
            DispatchInboundWorkAsync,
            HandleInboundShardFailure);
        if (senderConnection.ShardId < 0 ||
            senderConnection.ShardId >= shards.Count)
        {
            shardWork.Release();
            throw new InvalidOperationException(
                "The MQTT connection references an invalid event loop shard.");
        }

        try
        {
            await shards[senderConnection.ShardId].EnqueueAsync(
                shardWork,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            shardWork.Release();
            throw;
        }
    }

    private async ValueTask DispatchInboundWorkAsync(
        MqttZeroInboundWork work,
        CancellationToken cancellationToken)
    {
        await RoutePreparedLeaseAsync(
            work.Sender,
            work.SenderConnection!,
            work.Lease,
            cancellationToken).ConfigureAwait(false);
        await DeleteInboundDispatchAsync(
            work.Sender.ClientId,
            work.MessageId,
            cancellationToken).ConfigureAwait(false);
    }

    private void HandleInboundShardFailure(Exception exception)
    {
        MqttZeroBrokerLogging.LogInboundDispatcherFailure(
            _logger,
            exception);
        Volatile.Write(ref _stopping, 1);
        Volatile.Write(
            ref _state,
            (int)MqttZeroBrokerState.Stopping);
    }

    private static void ReleaseSessionMessages(MqttZeroSessionState session)
    {
        lock (session.SyncRoot)
        {
            foreach (MqttZeroQueuedMessage message in session.PendingMessages)
            {
                message.Release();
            }

            session.PendingMessages.Clear();
            foreach (MqttZeroQueuedMessage message in session.InflightMessages.Values)
            {
                message.Release();
            }

            session.InflightMessages.Clear();
            foreach (MqttZeroMessageLease inbound in session.InboundQos2Messages.Values)
            {
                inbound.Release();
            }

            session.InboundQos2Messages.Clear();
            session.RoutedInboundQos2Messages.Clear();
        }
    }

}
