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
/// MQTT 5 Broker 主体，按连接、会话、路由、投递与生命周期拆成多个分部文件。
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// UseFastFanoutDelivery 的值。
    /// </summary>
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

    /// <summary>
    /// 投递Planned。
    /// </summary>
    /// <param name="delivery">对应的 delivery。</param>
    /// <param name="lease">租约。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

        return EnqueueLeaseAsync(
            delivery.Session,
            lease,
            deliveryQos,
            retain,
            subscriptionIdentifiers,
            senderConnection,
            cancellationToken,
            copySubscriptionIdentifiers: true);
    }

    /// <summary>
    /// 处理Fanout失败。
    /// </summary>
    /// <param name="exception">异常。</param>
    private void HandleFanoutFailure(Exception exception)
    {
        MqttZeroBrokerLogging.LogFanoutFailure(
            _logger,
            exception);
        _fanoutDiagnostics.RecordFanoutFailure();
    }

    /// <summary>
    /// 路由ToSubscribers。
    /// </summary>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="lease">租约。</param>
    /// <param name="scheduleWithoutWaiting">对应的 scheduleWithoutWaiting。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <param name="publisherShardId">publisher分片Id。</param>
    /// <returns>异步操作的结果。</returns>
    private ValueTask<int> RouteToSubscribersAsync(
        MqttZeroSessionState? sender,
        MqttZeroClientConnection? senderConnection,
        MqttZeroMessageLease lease,
        bool scheduleWithoutWaiting,
        CancellationToken cancellationToken,
        int publisherShardId = 0)
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
        publisherShardId = sender?.ShardId ??
            senderConnection?.ShardId ??
            publisherShardId;
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

    /// <summary>
    /// 执行FanoutOn分片。
    /// </summary>
    /// <param name="shards">分片。</param>
    /// <param name="dispatcher">对应的 dispatcher。</param>
    /// <param name="lanes">通道。</param>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="lease">租约。</param>
    /// <param name="publisherShardId">publisher分片Id。</param>
    /// <param name="scheduleWithoutWaiting">对应的 scheduleWithoutWaiting。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
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

    /// <summary>
    /// 入队报文。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="source">来源。</param>
    /// <param name="qos">对应的 qos。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask EnqueuePacketAsync(
        MqttZeroSessionState session,
        MqttPublishPacket source,
        MqttQualityOfServiceLevel qos,
        bool retain,
        uint? subscriptionIdentifier,
        CancellationToken cancellationToken)
    {
        if (!subscriptionIdentifier.HasValue)
        {
            await EnqueuePacketAsync(
                session,
                source,
                qos,
                retain,
                ReadOnlyMemory<uint>.Empty,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        uint[] buffer = ArrayPool<uint>.Shared.Rent(1);
        try
        {
            buffer[0] = subscriptionIdentifier.Value;
            await EnqueuePacketAsync(
                session,
                source,
                qos,
                retain,
                new ReadOnlyMemory<uint>(buffer, 0, 1),
                cancellationToken,
                copySubscriptionIdentifiers: true).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<uint>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// 入队报文。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="source">来源。</param>
    /// <param name="qos">对应的 qos。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <param name="copySubscriptionIdentifiers">是否把标识符复制到队列消息自有缓冲区。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask EnqueuePacketAsync(
        MqttZeroSessionState session,
        MqttPublishPacket source,
        MqttQualityOfServiceLevel qos,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        CancellationToken cancellationToken,
        bool copySubscriptionIdentifiers = false)
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
            await EnqueueLeaseAsync(
                session,
                lease,
                qos,
                retain,
                subscriptionIdentifiers,
                null,
                cancellationToken,
                copySubscriptionIdentifiers).ConfigureAwait(false);
        }
        finally
        {
            lease.Release();
        }
    }

    /// <summary>
    /// 入队租约。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="lease">租约。</param>
    /// <param name="qos">对应的 qos。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask EnqueueLeaseAsync(
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qos,
        bool retain,
        uint? subscriptionIdentifier,
        MqttZeroClientConnection? senderConnection,
        CancellationToken cancellationToken)
    {
        if (!subscriptionIdentifier.HasValue)
        {
            await EnqueueLeaseAsync(
                session,
                lease,
                qos,
                retain,
                ReadOnlyMemory<uint>.Empty,
                senderConnection,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        uint[] buffer = ArrayPool<uint>.Shared.Rent(1);
        try
        {
            buffer[0] = subscriptionIdentifier.Value;
            await EnqueueLeaseAsync(
                session,
                lease,
                qos,
                retain,
                new ReadOnlyMemory<uint>(buffer, 0, 1),
                senderConnection,
                cancellationToken,
                copySubscriptionIdentifiers: true).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<uint>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// 入队租约。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="lease">租约。</param>
    /// <param name="qos">对应的 qos。</param>
    /// <param name="retain">保留。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <param name="copySubscriptionIdentifiers">copy订阅标识符。</param>
    /// <returns>表示异步操作的任务。</returns>
    /// <remarks>
    /// 必须在会话所属分片上调用。会话连接在每次 await 之后重新读取，因为连接可能在等待期间被接管或断开。
    /// </remarks>
    private async ValueTask EnqueueLeaseAsync(
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qos,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        MqttZeroClientConnection? senderConnection,
        CancellationToken cancellationToken,
        bool copySubscriptionIdentifiers = false)
    {
        EnsureSessionShard(
            session,
            nameof(EnqueueLeaseAsync));
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
                    session.OfflineClientInfo,
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
                    session.Connection?.ClientInfo ?? session.OfflineClientInfo,
                    evicted,
                    cancellationToken).ConfigureAwait(false);
            }

            if (messageHeld && message != null && _messageStore.IsEnabled)
            {
                try
                {
                    if (evicted != null)
                    {
                        await ReplaceQueuedAsync(
                            session.ClientId,
                            evicted.MessageId,
                            message,
                            cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await SaveQueuedAsync(
                            message,
                            session.ClientId,
                            cancellationToken).ConfigureAwait(false);
                    }
                }
                catch
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

                    message.Release();
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
                    session.OfflineClientInfo;
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

    /// <summary>
    /// 入队消息。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="message">消息。</param>
    /// <returns>对应的结果。</returns>
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
        RescheduleSessionMessageExpiry(session);
        _fanoutDiagnostics.RecordPendingQueueDepth(
            session.PendingMessages.Count);
        return evicted;
    }

    /// <summary>
    /// 通知出站。
    /// </summary>
    /// <param name="session">会话。</param>
    private void SignalEgress(MqttZeroSessionState session)
    {
        MqttZeroSessionEgress? egress = session.Egress;
        if (egress == null)
        {
            return;
        }

        _fanoutDiagnostics.RecordEgressSignal();
        egress.Signal();
    }

    /// <summary>
    /// 通知NotConsumed。
    /// </summary>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="lease">租约。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 通知Dropped。
    /// </summary>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="session">会话。</param>
    /// <param name="lease">租约。</param>
    /// <param name="reason">原因。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 通知Enqueued。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 处理出站PubRec。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ProcessOutboundPubRecAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        EnsureSessionShard(
            session,
            nameof(ProcessOutboundPubRecAsync));
        MqttZeroQueuedMessage? message;
        session.InflightMessages.TryGetValue(
            packetIdentifier,
            out message);

        if (message == null ||
            message.QualityOfServiceLevel != MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return;
        }

        if (reasonCode >= MqttReasonCode.UnspecifiedError)
        {
            MqttZeroQueuedMessage? failed = null;
            session.InflightMessages.Remove(
                packetIdentifier,
                out failed);

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
                    session.InflightMessages[packetIdentifier] =
                        failed;

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

    /// <summary>
    /// 处理出站PubComp。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ProcessOutboundPubCompAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        EnsureSessionShard(
            session,
            nameof(ProcessOutboundPubCompAsync));
        MqttZeroQueuedMessage? completed = null;
        bool protocolViolation = false;
        if (session.InflightMessages.TryGetValue(
                packetIdentifier,
                out completed))
        {
            if (completed.QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.ExactlyOnce)
            {
                session.InflightMessages.Remove(packetIdentifier);
            }
            else
            {
                completed = null;
                protocolViolation = true;
            }
        }

        if (protocolViolation)
        {
            await connection.CloseAsync(
                MqttReasonCode.ProtocolError,
                cancellationToken).ConfigureAwait(false);
            return;
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
                session.InflightMessages[packetIdentifier] =
                    completed;

                throw;
            }

            completed.Release();
        }

        SignalEgress(session);
    }

    /// <summary>
    /// 处理入站PubRel。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ProcessInboundPubRelAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        EnsureSessionShard(
            session,
            nameof(ProcessInboundPubRelAsync));
        MqttZeroMessageLease? lease;
        MqttReasonCode completionReason = MqttReasonCode.Success;
        bool routeCompleted;
        bool stateRemoved = false;
        session.InboundQos2Messages.TryGetValue(
            packetIdentifier,
            out lease);
        routeCompleted = session.RoutedInboundQos2Messages.TryGetValue(
            packetIdentifier,
            out completionReason);

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
            session.InboundQos2Messages.Remove(packetIdentifier);
            session.RoutedInboundQos2Messages.Remove(packetIdentifier);

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
                session.RoutedInboundQos2Messages[packetIdentifier] =
                    completionReason;
            }

            await DeleteInboundQos2Async(
                session.ClientId,
                packetIdentifier,
                cancellationToken).ConfigureAwait(false);
            session.InboundQos2Messages.Remove(packetIdentifier);
            session.RoutedInboundQos2Messages.Remove(packetIdentifier);
            stateRemoved = true;

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

    /// <summary>
    /// 通知Overwritten。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="receiver">对应的 receiver。</param>
    /// <param name="evicted">对应的 evicted。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 启动出站。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>对应的结果。</returns>
    internal MqttZeroSessionEgress StartEgress(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        CancellationToken cancellationToken)
    {
        MqttZeroSessionEgress? egress = null;
        MqttZeroSessionEgressServices services = new(
            _options,
            _timeProvider,
            _fanoutDiagnostics,
            DeleteMessageAsync,
            (targetSession, lease, reason, token) =>
                NotifyDroppedAsync(
                    null,
                    targetSession,
                    lease,
                    reason,
                    token),
            TransitionQueuedToOutboundAsync,
            RescheduleSessionMessageExpiry);
        egress = new MqttZeroSessionEgress(
            session,
            connection,
            ShardSet.GetEgressScheduler(session.ShardId),
            services,
            OnEgressFailure);
        if (!session.TryAttachEgress(connection, egress))
        {
            throw new InvalidOperationException(
                "The MQTT session already has an outbound pump.");
        }

        connection.AttachEgress(egress);
        egress.Signal();
        return egress;

        void OnEgressFailure(Exception exception)
        {
            MqttZeroBrokerLogging.LogEgressFailure(
                _logger,
                session.ClientId,
                exception);
            if (ReferenceEquals(session.Egress, egress))
            {
                session.TakeEgress();
            }

            connection.Abort();
        }
    }

    /// <summary>
    /// CanDispatchInbound 的值。
    /// </summary>
    internal bool CanDispatchInbound
    {
        get
        {
            return _shardSet != null;
        }
    }

    /// <summary>
    /// CanDispatchInboundQos2 的值。
    /// </summary>
    private bool CanDispatchInboundQos2
    {
        get
        {
            return CanDispatchInbound && !_messageStore.IsEnabled;
        }
    }

    /// <summary>
    /// 入队入站。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <param name="messageId">消息Id。</param>
    /// <returns>表示异步操作的任务。</returns>
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
            HandleConnectionWorkFailure);
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

    /// <summary>
    /// 分发入站工作项。
    /// </summary>
    /// <param name="work">工作项。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask DispatchInboundWorkAsync(
        MqttZeroInboundWork work,
        CancellationToken cancellationToken)
    {
        await RoutePreparedLeaseAsync(
            work.Sender,
            work.SenderConnection,
            work.Lease,
            cancellationToken).ConfigureAwait(false);
        await DeleteInboundDispatchAsync(
            work.Sender.ClientId,
            work.MessageId,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 处理连接工作项失败。
    /// </summary>
    /// <param name="exception">异常。</param>
    internal void HandleConnectionWorkFailure(Exception exception)
    {
        MqttZeroBrokerLogging.LogInboundDispatcherFailure(
            _logger,
            exception);
    }

    /// <summary>
    /// 释放会话消息。
    /// </summary>
    /// <param name="session">会话。</param>
    private static void ReleaseSessionMessagesCore(
        MqttZeroSessionState session)
    {
        foreach (MqttZeroQueuedMessage message in session.PendingMessages)
        {
            message.Release();
        }

        session.PendingMessages.Clear();
        foreach (MqttZeroQueuedMessage message in
            session.InflightMessages.Values)
        {
            message.Release();
        }

        session.InflightMessages.Clear();
        foreach (MqttZeroMessageLease inbound in
            session.InboundQos2Messages.Values)
        {
            inbound.Release();
        }

        session.InboundQos2Messages.Clear();
        session.RoutedInboundQos2Messages.Clear();
    }

    /// <summary>
    /// 释放会话消息On分片。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ReleaseSessionMessagesOnShardAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        MqttZeroShardSet? shards = Volatile.Read(ref _shardSet);
        if (shards == null)
        {
            ReleaseSessionMessagesCore(session);
            return;
        }

        MqttZeroEventLoop shard = shards[session.ShardId];
        if (shard.IsCurrent)
        {
            ReleaseSessionMessagesCore(session);
            return;
        }

        await shard.InvokeAsync(
            _ =>
            {
                ReleaseSessionMessagesCore(session);
                return ValueTask.FromResult(true);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 释放全部会话消息On分片。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ReleaseAllSessionMessagesOnShardsAsync(
        CancellationToken cancellationToken)
    {
        MqttZeroShardSet? shards = Volatile.Read(ref _shardSet);
        if (shards == null)
        {
            foreach (MqttZeroSessionState session in _sessions.Values)
            {
                ReleaseSessionMessagesCore(session);
            }

            return;
        }

        for (int shardId = 0; shardId < shards.Count; shardId++)
        {
            await shards[shardId].InvokeAsync(
                _ =>
                {
                    foreach (MqttZeroSessionState session in
                        _sessions.Values)
                    {
                        if (session.ShardId == shardId)
                        {
                            ReleaseSessionMessagesCore(session);
                        }
                    }

                    return ValueTask.FromResult(true);
                },
                cancellationToken).ConfigureAwait(false);
        }
    }

}
