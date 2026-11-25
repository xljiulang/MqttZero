using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Retained;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Text;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private async ValueTask RestorePersistenceAsync(CancellationToken cancellationToken)
    {
        Dictionary<string, MqttZeroSessionState> stagedSessions =
            new(StringComparer.Ordinal);
        Dictionary<string, MqttZeroRetainedMessage> stagedRetainedMessages =
            new(StringComparer.Ordinal);
        MqttZeroRetainedTopicTrie stagedRetainedTrie =
            MqttZeroRetainedTopicTrie.Empty;
        List<(
            MqttZeroSessionState Session,
            MqttZeroMessageLease Lease,
            long MessageId)> inboundDispatches = [];
        bool inboundDispatchesReleased = true;
        long stagedRetainedBytes = 0;
        bool committed = false;
        try
        {
            IReadOnlyList<MqttZeroPersistedSession> sessions =
                await _sessionStore.LoadAllAsync(cancellationToken).ConfigureAwait(false);
            foreach (MqttZeroPersistedSession persisted in sessions)
            {
                if (string.IsNullOrEmpty(persisted.ClientId))
                {
                    continue;
                }

                if (stagedSessions.ContainsKey(persisted.ClientId))
                {
                    continue;
                }

                DateTimeOffset now = _timeProvider.GetUtcNow();
                if (persisted.ExpiryInterval > 0 &&
                    persisted.DisconnectedAt.HasValue &&
                    persisted.DisconnectedAt.Value.AddSeconds(persisted.ExpiryInterval) <= now &&
                    persisted.Will == null)
                {
                    await DeleteClientPersistenceAsync(
                        persisted.ClientId,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (stagedSessions.Count >= _options.MaxSessions)
                {
                    await DeleteClientPersistenceAsync(
                        persisted.ClientId,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (persisted.Will != null && !IsValidPersistedWill(persisted.Will))
                {
                    await DeleteClientPersistenceAsync(
                        persisted.ClientId,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                MqttZeroSessionState session = CreateSessionState(
                    persisted.ClientId);
                session.CleanStart = persisted.CleanStart;
                session.ExpiryInterval = persisted.ExpiryInterval;
                session.DisconnectedAt = persisted.DisconnectedAt;
                session.WillMessage = persisted.Will == null
                    ? null
                    : new MqttZeroWillMessage(persisted.Will);
                if (session.DisconnectedAt.HasValue &&
                    session.WillMessage != null)
                {
                    uint delay = session.WillMessage.DelayInterval;
                    if (session.ExpiryInterval > 0)
                    {
                        delay = Math.Min(delay, session.ExpiryInterval);
                    }

                    session.WillDueAt = session.DisconnectedAt.Value.AddSeconds(delay);
                }

                bool validSession = true;
                foreach (MqttZeroPersistedSubscription subscription in persisted.Subscriptions)
                {
                    if (!IsValidSubscription(subscription))
                    {
                        validSession = false;
                        break;
                    }

                    if (session.Subscriptions.Count >= _options.MaxSubscriptionsPerSession)
                    {
                        validSession = false;
                        break;
                    }

                    session.Subscriptions.Add(new MqttZeroSubscription(
                        subscription.TopicFilter.ToArray(),
                        subscription.QualityOfServiceLevel,
                        subscription.NoLocal,
                        subscription.RetainAsPublished,
                        subscription.SubscriptionIdentifier));
                }

                if (!validSession)
                {
                    await DeleteClientPersistenceAsync(
                        persisted.ClientId,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                stagedSessions.Add(session.ClientId, session);
            }

            IReadOnlyList<MqttZeroPersistedMessage> messages =
                await _messageStore.LoadAllAsync(cancellationToken).ConfigureAwait(false);
            HashSet<string> orphanClients = new(StringComparer.Ordinal);
            foreach (MqttZeroPersistedMessage persisted in messages)
            {
                if (!stagedSessions.TryGetValue(
                        persisted.ClientId,
                        out MqttZeroSessionState? session))
                {
                    if (!string.IsNullOrEmpty(persisted.ClientId))
                    {
                        orphanClients.Add(persisted.ClientId);
                    }

                    continue;
                }

                if (!IsValidPersistedMessage(persisted) ||
                    (persisted.ExpiresAt.HasValue &&
                        persisted.ExpiresAt.Value <= _timeProvider.GetUtcNow()))
                {
                    await DeletePersistedMessageAsync(persisted, cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                if (persisted.Kind == MqttZeroPersistedMessageKind.InboundQos2)
                {
                    if (session.InboundQos2Messages.ContainsKey(persisted.PacketIdentifier) ||
                        session.InboundQos2Messages.Count >= _options.MaxAwaitingRel)
                    {
                        await DeletePersistedMessageAsync(persisted, cancellationToken)
                            .ConfigureAwait(false);
                        continue;
                    }

                    MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                        new ReadOnlySequence<byte>(persisted.Topic),
                        NormalizePersistedMessageProperties(persisted.Properties),
                        new ReadOnlySequence<byte>(persisted.Payload),
                        persisted.QualityOfServiceLevel,
                        persisted.Retain,
                        null,
                        null,
                        _bufferPool);
                    lease.SetExpiresAt(persisted.ExpiresAt);
                    lease.PacketIdentifier = persisted.PacketIdentifier;
                    try
                    {
                        session.InboundQos2Messages.Add(persisted.PacketIdentifier, lease);
                    }
                    catch
                    {
                        lease.Release();
                        throw;
                    }

                    continue;
                }

                if (persisted.Kind ==
                    MqttZeroPersistedMessageKind.InboundDispatch)
                {
                    MqttZeroMessageLease dispatchLease =
                        MqttZeroMessageLease.CopyFrom(
                            new ReadOnlySequence<byte>(persisted.Topic),
                            NormalizePersistedMessageProperties(
                                persisted.Properties),
                            new ReadOnlySequence<byte>(persisted.Payload),
                            persisted.QualityOfServiceLevel,
                            persisted.Retain,
                            null,
                            null,
                            _bufferPool);
                    dispatchLease.SetExpiresAt(persisted.ExpiresAt);
                    dispatchLease.PacketIdentifier =
                        persisted.PacketIdentifier;
                    inboundDispatches.Add((
                        session,
                        dispatchLease,
                        persisted.MessageId));
                    inboundDispatchesReleased = false;
                    continue;
                }

                if ((persisted.Kind == MqttZeroPersistedMessageKind.Outbound &&
                        (session.InflightMessages.ContainsKey(persisted.PacketIdentifier) ||
                            session.InflightMessages.Count >= _options.MaxInflight)) ||
                    (persisted.Kind == MqttZeroPersistedMessageKind.Queued &&
                        session.PendingMessages.Count >= _options.MaxMessageQueueLength))
                {
                    await DeletePersistedMessageAsync(persisted, cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                MqttZeroMessageLease queuedLease = MqttZeroMessageLease.CopyFrom(
                    new ReadOnlySequence<byte>(persisted.Topic),
                    NormalizePersistedMessageProperties(persisted.Properties),
                    new ReadOnlySequence<byte>(persisted.Payload),
                    persisted.QualityOfServiceLevel,
                    persisted.Retain,
                    null,
                    null,
                    _bufferPool);
                MqttZeroQueuedMessage message;
                try
                {
                    message = MqttZeroQueuedMessage.RentWithIdentifiers(
                        queuedLease,
                        persisted.QualityOfServiceLevel,
                        persisted.Retain,
                        persisted.SubscriptionIdentifiers,
                        persisted.ExpiresAt);
                }
                finally
                {
                    queuedLease.Release();
                }

                try
                {
                    MqttZeroOutboundState state = persisted.State switch
                    {
                        MqttZeroPersistedDeliveryState.AwaitPubComp =>
                            MqttZeroOutboundState.AwaitPubComp,
                        MqttZeroPersistedDeliveryState.AwaitPubRec =>
                            MqttZeroOutboundState.AwaitPubRec,
                        _ => MqttZeroOutboundState.AwaitPubAck
                    };
                    message.Restore(
                        persisted.MessageId,
                        persisted.PacketIdentifier,
                        state);
                    if (persisted.Kind == MqttZeroPersistedMessageKind.Outbound)
                    {
                        session.InflightMessages.Add(message.PacketIdentifier, message);
                    }
                    else
                    {
                        session.PendingMessages.Add(message);
                    }
                }
                catch
                {
                    message.Release();
                    throw;
                }
            }

            if (_messageStore.IsEnabled)
            {
                foreach (string clientId in orphanClients)
                {
                    await DeleteClientMessagesAsync(clientId, cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            IReadOnlyList<MqttZeroPersistedRetainedMessage> retained =
                await _retainedMessageStore.LoadAllAsync(cancellationToken).ConfigureAwait(false);
            foreach (MqttZeroPersistedRetainedMessage persisted in retained)
            {
                string topic = Encoding.UTF8.GetString(persisted.Topic.Span);
                if (!IsValidRetainedMessage(persisted) ||
                    (persisted.ExpiresAt.HasValue &&
                        persisted.ExpiresAt.Value <= _timeProvider.GetUtcNow()) ||
                    stagedRetainedMessages.ContainsKey(topic))
                {
                    await DeleteRetainedAsync(topic, cancellationToken)
                        .ConfigureAwait(false);

                    continue;
                }

                long retainedBytes =
                    persisted.Topic.Length +
                    persisted.Properties.Length +
                    persisted.Payload.Length;
                if ((_options.MaxRetainedMessages > 0 &&
                        stagedRetainedMessages.Count >= _options.MaxRetainedMessages) ||
                    stagedRetainedBytes + retainedBytes > _options.MaxRetainedBytes)
                {
                    await DeleteRetainedAsync(topic, cancellationToken)
                        .ConfigureAwait(false);

                    continue;
                }

                MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    new ReadOnlySequence<byte>(persisted.Topic),
                    NormalizePersistedMessageProperties(persisted.Properties),
                    new ReadOnlySequence<byte>(persisted.Payload),
                    persisted.QualityOfServiceLevel,
                    true,
                    null,
                    null,
                    _bufferPool);
                MqttZeroRetainedMessage? retainedMessage = null;
                try
                {
                    retainedMessage = new MqttZeroRetainedMessage(
                        lease,
                        persisted.QualityOfServiceLevel,
                        persisted.ExpiresAt);
                    stagedRetainedMessages.Add(topic, retainedMessage);
                    stagedRetainedTrie = stagedRetainedTrie.Set(
                        retainedMessage.Topic.Span,
                        retainedMessage);
                    stagedRetainedBytes += retainedMessage.ByteLength;
                    retainedMessage = null;
                }
                finally
                {
                    lease.Release();
                    retainedMessage?.Release();
                }
            }

            lock (_sessionGate)
            {
                _sessions.Clear();
                foreach (KeyValuePair<string, MqttZeroSessionState> pair in stagedSessions)
                {
                    _sessions[pair.Key] = pair.Value;
                }

                _retainedMessages.Clear();
                foreach (KeyValuePair<string, MqttZeroRetainedMessage> pair in
                    stagedRetainedMessages)
                {
                    _retainedMessages[pair.Key] = pair.Value;
                }

                Volatile.Write(
                    ref _retainedTopicTrie,
                    stagedRetainedTrie);
                _retainedBytes = stagedRetainedBytes;
            }

            ResetRouteEpochFromSessions();
            while (inboundDispatches.Count > 0)
            {
                int dispatchIndex = 0;
                for (int index = 1;
                    index < inboundDispatches.Count;
                    index++)
                {
                    if (CompareInboundDispatchOrder(
                        inboundDispatches[index],
                        inboundDispatches[dispatchIndex]) < 0)
                    {
                        dispatchIndex = index;
                    }
                }

                (
                    MqttZeroSessionState Session,
                    MqttZeroMessageLease Lease,
                    long MessageId) dispatch =
                    inboundDispatches[dispatchIndex];
                inboundDispatches.RemoveAt(dispatchIndex);
                try
                {
                    await RoutePreparedLeaseAsync(
                        dispatch.Session,
                        null,
                        dispatch.Lease,
                        cancellationToken).ConfigureAwait(false);
                    await DeleteInboundDispatchAsync(
                        dispatch.Session.ClientId,
                        dispatch.MessageId,
                        cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    dispatch.Lease.Release();
                }
            }

            inboundDispatchesReleased = true;

            committed = true;
        }
        catch (Exception exception)
        {
            lock (_sessionGate)
            {
                _sessions.Clear();
                _retainedMessages.Clear();
                Volatile.Write(
                    ref _retainedTopicTrie,
                    MqttZeroRetainedTopicTrie.Empty);
                _retainedBytes = 0;
                _routeEpochs.Reset(MqttZeroRouteEpoch.Empty);
            }

            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(RestorePersistenceAsync),
                string.Empty,
                exception);
            throw;
        }
        finally
        {
            if (!committed)
            {
                if (!inboundDispatchesReleased)
                {
                    foreach ((
                        _,
                        MqttZeroMessageLease lease,
                        _) in inboundDispatches)
                    {
                        lease.Release();
                    }
                }

                foreach (MqttZeroSessionState session in stagedSessions.Values)
                {
                    ReleaseSessionMessages(session);
                }

                foreach (MqttZeroRetainedMessage retained in stagedRetainedMessages.Values)
                {
                    retained.Release();
                }
            }
        }
    }

    private async ValueTask DeletePersistedMessageAsync(
        MqttZeroPersistedMessage message,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            if (message.Kind == MqttZeroPersistedMessageKind.InboundQos2)
            {
                await _messageStore.DeleteInboundQos2Async(
                    message.ClientId,
                    message.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _messageStore.DeleteMessageAsync(
                    message.ClientId,
                    message.MessageId,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(DeletePersistedMessageAsync),
                message.ClientId,
                exception);
            throw;
        }
    }

    private static bool IsValidPersistedWill(MqttZeroPersistedWill will)
    {
        return IsValidPublishTopic(will.Topic) &&
            IsValidQualityOfService(will.QualityOfServiceLevel);
    }

    private static bool IsValidSubscription(
        MqttZeroPersistedSubscription subscription)
    {
        return MqttZeroProtocolValidator.IsValidTopicFilter(
                new ReadOnlySequence<byte>(subscription.TopicFilter)) &&
            IsValidQualityOfService(subscription.QualityOfServiceLevel) &&
            (!subscription.SubscriptionIdentifier.HasValue ||
                subscription.SubscriptionIdentifier.Value != 0);
    }

    private static bool IsValidPersistedMessage(MqttZeroPersistedMessage message)
    {
        if (!IsValidPublishTopic(message.Topic) ||
            !IsValidQualityOfService(message.QualityOfServiceLevel) ||
            !Enum.IsDefined(message.Kind) ||
            !Enum.IsDefined(message.State) ||
            message.SubscriptionIdentifiers.Span.IndexOf((uint)0) >= 0)
        {
            return false;
        }

        if (message.Kind == MqttZeroPersistedMessageKind.InboundQos2)
        {
            return message.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.ExactlyOnce &&
                message.State == MqttZeroPersistedDeliveryState.Queued &&
                message.PacketIdentifier != 0;
        }

        if (message.Kind == MqttZeroPersistedMessageKind.InboundDispatch)
        {
            return message.MessageId > 0 &&
                message.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce &&
                message.State == MqttZeroPersistedDeliveryState.Queued &&
                message.PacketIdentifier != 0;
        }

        if (message.MessageId <= 0)
        {
            return false;
        }

        if (message.Kind == MqttZeroPersistedMessageKind.Queued)
        {
            return message.State == MqttZeroPersistedDeliveryState.AwaitPubAck &&
                (message.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce ||
                    message.PacketIdentifier != 0);
        }

        return message.Kind == MqttZeroPersistedMessageKind.Outbound &&
            message.PacketIdentifier != 0 &&
            message.State switch
            {
                MqttZeroPersistedDeliveryState.AwaitPubAck =>
                    message.QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.AtLeastOnce,
                MqttZeroPersistedDeliveryState.AwaitPubRec or
                    MqttZeroPersistedDeliveryState.AwaitPubComp =>
                    message.QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.ExactlyOnce,
                _ => false
            };
    }

    private static bool IsValidRetainedMessage(
        MqttZeroPersistedRetainedMessage message)
    {
        return IsValidPublishTopic(message.Topic) &&
            IsValidQualityOfService(message.QualityOfServiceLevel);
    }

    private static bool IsValidPublishTopic(ReadOnlyMemory<byte> topic)
    {
        return MqttZeroProtocolValidator.IsValidTopicName(
            new ReadOnlySequence<byte>(topic));
    }

    private static bool IsValidQualityOfService(
        MqttQualityOfServiceLevel qualityOfServiceLevel)
    {
        return (byte)qualityOfServiceLevel <=
            (byte)MqttQualityOfServiceLevel.ExactlyOnce;
    }

    private async ValueTask SaveSessionAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        await SaveSessionAsync(
            session,
            null,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask SaveSessionSubscriptionsAsync(
        MqttZeroSessionState session,
        IReadOnlyList<MqttZeroSubscription> subscriptions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        await SaveSessionAsync(
            session,
            subscriptions,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask SaveSessionAsync(
        MqttZeroSessionState session,
        IReadOnlyList<MqttZeroSubscription>? candidateSubscriptions,
        CancellationToken cancellationToken)
    {
        if (!_sessionStore.IsEnabled)
        {
            return;
        }

        List<MqttZeroPersistedSubscription> subscriptions = [];
        bool cleanStart;
        uint expiryInterval;
        DateTimeOffset? disconnectedAt;
        MqttZeroPersistedWill? will;
        lock (session.SyncRoot)
        {
            cleanStart = session.CleanStart;
            expiryInterval = session.ExpiryInterval;
            disconnectedAt = session.DisconnectedAt;
            will = CreatePersistedWill(session.WillMessage);
            IReadOnlyList<MqttZeroSubscription> source =
                candidateSubscriptions ?? session.Subscriptions;
            for (int index = 0; index < source.Count; index++)
            {
                MqttZeroSubscription subscription = source[index];
                subscriptions.Add(new MqttZeroPersistedSubscription
                {
                    TopicFilter = subscription.TopicFilter,
                    QualityOfServiceLevel = subscription.QualityOfServiceLevel,
                    NoLocal = subscription.NoLocal,
                    RetainAsPublished = subscription.RetainAsPublished,
                    SubscriptionIdentifier = subscription.SubscriptionIdentifier
                });
            }
        }

        try
        {
            await _sessionStore.SaveAsync(
                new MqttZeroPersistedSession
                {
                    ClientId = session.ClientId,
                    CleanStart = cleanStart,
                    ExpiryInterval = expiryInterval,
                    DisconnectedAt = disconnectedAt,
                    Will = will,
                    Subscriptions = subscriptions
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroSessionStore.SaveAsync),
                session.ClientId,
                exception);
            throw;
        }
    }

    private async ValueTask SaveRetainedAsync(
        MqttZeroMessageLease lease,
        DateTimeOffset? expiresAt,
        CancellationToken cancellationToken)
    {
        if (!_retainedMessageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _retainedMessageStore.SaveAsync(
                new MqttZeroPersistedRetainedMessage
                {
                    Topic = lease.Topic,
                    Properties = lease.Properties,
                    Payload = lease.Payload,
                    QualityOfServiceLevel = lease.QualityOfServiceLevel,
                    ExpiresAt = expiresAt
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroRetainedMessageStore.SaveAsync),
                Encoding.UTF8.GetString(lease.Topic.Span),
                exception);
            throw;
        }
    }

    private async ValueTask SaveQueuedAsync(
        MqttZeroQueuedMessage message,
        string clientId,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.SaveQueuedAsync(
                CreatePersistedMessage(
                    message,
                    clientId,
                    MqttZeroPersistedMessageKind.Queued),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.SaveQueuedAsync),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask SaveOutboundAsync(
        MqttZeroQueuedMessage message,
        string clientId,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.SaveOutboundAsync(
                CreatePersistedMessage(
                    message,
                    clientId,
                    MqttZeroPersistedMessageKind.Outbound),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.SaveOutboundAsync),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask SaveInboundQos2Async(
        MqttZeroMessageLease lease,
        string clientId,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.SaveInboundQos2Async(
                CreateInboundPersistedMessage(lease, clientId, packetIdentifier),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.SaveInboundQos2Async),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask SaveInboundDispatchAsync(
        MqttZeroMessageLease lease,
        MqttZeroSessionState sender,
        long messageId,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.SaveInboundDispatchAsync(
                CreateInboundDispatchPersistedMessage(
                    lease,
                    sender.ClientId,
                    messageId),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.SaveInboundDispatchAsync),
                sender.ClientId,
                exception);
            throw;
        }
    }

    private async ValueTask DeleteInboundDispatchAsync(
        string clientId,
        long messageId,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled || messageId == 0)
        {
            return;
        }

        try
        {
            await _messageStore.DeleteInboundDispatchAsync(
                clientId,
                messageId,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.DeleteInboundDispatchAsync),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask TransitionQueuedToOutboundAsync(
        MqttZeroQueuedMessage message,
        string clientId,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.TransitionQueuedToOutboundAsync(
                clientId,
                message.MessageId,
                CreatePersistedMessage(
                    message,
                    clientId,
                    MqttZeroPersistedMessageKind.Outbound),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.TransitionQueuedToOutboundAsync),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask ReplaceQueuedAsync(
        string clientId,
        long replacedMessageId,
        MqttZeroQueuedMessage message,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.ReplaceQueuedAsync(
                clientId,
                replacedMessageId,
                CreatePersistedMessage(
                    message,
                    clientId,
                    MqttZeroPersistedMessageKind.Queued),
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.ReplaceQueuedAsync),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask DeleteMessageAsync(
        string clientId,
        long messageId,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.DeleteMessageAsync(
                clientId,
                messageId,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.DeleteMessageAsync),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask DeleteInboundQos2Async(
        string clientId,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.DeleteInboundQos2Async(
                clientId,
                packetIdentifier,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.DeleteInboundQos2Async),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask DeleteClientMessagesAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _messageStore.DeleteByClientAsync(clientId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroMessageStore.DeleteByClientAsync),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask DeleteSessionAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        if (!_sessionStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _sessionStore.DeleteAsync(clientId, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroSessionStore.DeleteAsync),
                clientId,
                exception);
            throw;
        }
    }

    private async ValueTask DeleteRetainedAsync(
        string topic,
        CancellationToken cancellationToken)
    {
        if (!_retainedMessageStore.IsEnabled)
        {
            return;
        }

        try
        {
            await _retainedMessageStore.DeleteAsync(topic, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroRetainedMessageStore.DeleteAsync),
                topic,
                exception);
            throw;
        }
    }

    private static MqttZeroPersistedMessage CreatePersistedMessage(
        MqttZeroQueuedMessage message,
        string clientId,
        MqttZeroPersistedMessageKind kind)
    {
        return new MqttZeroPersistedMessage
        {
            ClientId = clientId,
            MessageId = message.MessageId,
            PacketIdentifier = message.PacketIdentifier,
            Kind = kind,
            State = message.State switch
            {
                MqttZeroOutboundState.AwaitPubComp =>
                    MqttZeroPersistedDeliveryState.AwaitPubComp,
                MqttZeroOutboundState.AwaitPubRec =>
                    MqttZeroPersistedDeliveryState.AwaitPubRec,
                _ => MqttZeroPersistedDeliveryState.AwaitPubAck
            },
            Topic = message.Topic,
            Properties = message.Properties,
            Payload = message.Payload,
            QualityOfServiceLevel = message.QualityOfServiceLevel,
            Retain = message.Retain,
            SubscriptionIdentifiers = message.SubscriptionIdentifiers,
            ExpiresAt = message.ExpiresAt
        };
    }

    private static MqttZeroPersistedMessage CreateInboundPersistedMessage(
        MqttZeroMessageLease lease,
        string clientId,
        ushort packetIdentifier)
    {
        return new MqttZeroPersistedMessage
        {
            ClientId = clientId,
            MessageId = packetIdentifier,
            PacketIdentifier = packetIdentifier,
            Kind = MqttZeroPersistedMessageKind.InboundQos2,
            State = MqttZeroPersistedDeliveryState.Queued,
            Topic = lease.Topic,
            Properties = lease.Properties,
            Payload = lease.Payload,
            QualityOfServiceLevel = lease.QualityOfServiceLevel,
            Retain = lease.RetainFlag,
            ExpiresAt = lease.ExpiresAt
        };
    }

    private static MqttZeroPersistedMessage CreateInboundDispatchPersistedMessage(
        MqttZeroMessageLease lease,
        string clientId,
        long messageId)
    {
        return new MqttZeroPersistedMessage
        {
            ClientId = clientId,
            MessageId = messageId,
            PacketIdentifier = lease.PacketIdentifier,
            Kind = MqttZeroPersistedMessageKind.InboundDispatch,
            State = MqttZeroPersistedDeliveryState.Queued,
            Topic = lease.Topic,
            Properties = lease.Properties,
            Payload = lease.Payload,
            QualityOfServiceLevel = lease.QualityOfServiceLevel,
            Retain = lease.RetainFlag,
            ExpiresAt = lease.ExpiresAt
        };
    }

    private static int CompareInboundDispatchOrder(
        (
            MqttZeroSessionState Session,
            MqttZeroMessageLease Lease,
            long MessageId) left,
        (
            MqttZeroSessionState Session,
            MqttZeroMessageLease Lease,
            long MessageId) right)
    {
        int clientComparison = string.CompareOrdinal(
            left.Session.ClientId,
            right.Session.ClientId);
        return clientComparison != 0
            ? clientComparison
            : left.MessageId.CompareTo(right.MessageId);
    }

    private static ReadOnlySequence<byte> NormalizePersistedMessageProperties(
        ReadOnlyMemory<byte> properties)
    {
        if (properties.IsEmpty)
        {
            return ReadOnlySequence<byte>.Empty;
        }

        if (!MqttPublishPropertiesCodec.TryDecode(
                new ReadOnlySequence<byte>(properties),
                out MqttPublishProperties publishProperties,
                out _,
                out _))
        {
            throw new InvalidOperationException(
                "The persisted MQTT PUBLISH properties are invalid.");
        }

        return MqttPublishPropertiesCodec.Encode(publishProperties);
    }

    private DateTimeOffset? CalculateMessageExpiresAt(uint? expiryInterval)
    {
        return expiryInterval.HasValue
            ? _timeProvider.GetUtcNow().AddSeconds(expiryInterval.Value)
            : null;
    }

    private static uint? GetRemainingMessageExpiry(
        DateTimeOffset? expiresAt,
        DateTimeOffset now)
    {
        if (!expiresAt.HasValue)
        {
            return null;
        }

        double remainingSeconds = Math.Ceiling((expiresAt.Value - now).TotalSeconds);
        if (remainingSeconds <= 0)
        {
            return 0;
        }

        return (uint)Math.Min(remainingSeconds, uint.MaxValue);
    }

    private static MqttZeroPersistedWill? CreatePersistedWill(MqttZeroWillMessage? will)
    {
        if (will == null)
        {
            return null;
        }

        return new MqttZeroPersistedWill
        {
            Topic = will.Topic,
            Payload = will.Payload,
            Properties = will.Properties,
            QualityOfServiceLevel = will.QualityOfServiceLevel,
            Retain = will.Retain,
            DelayInterval = will.DelayInterval,
            MessageExpiryInterval = will.MessageExpiryInterval
        };
    }

    private async ValueTask DeleteClientPersistenceAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        await DeleteClientMessagesAsync(clientId, cancellationToken).ConfigureAwait(false);
        await DeleteSessionAsync(clientId, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask RestoreSessionPersistenceAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        await SaveSessionAsync(session, cancellationToken).ConfigureAwait(false);
        MqttZeroQueuedMessage[] pending;
        MqttZeroQueuedMessage[] inflight;
        KeyValuePair<ushort, MqttZeroMessageLease>[] inboundQos2;
        lock (session.SyncRoot)
        {
            pending = [.. session.PendingMessages];
            inflight = [.. session.InflightMessages.Values];
            inboundQos2 = [.. session.InboundQos2Messages];
        }

        foreach (MqttZeroQueuedMessage message in pending)
        {
            await SaveQueuedAsync(message, session.ClientId, cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (MqttZeroQueuedMessage message in inflight)
        {
            await SaveOutboundAsync(message, session.ClientId, cancellationToken)
                .ConfigureAwait(false);
        }

        foreach (KeyValuePair<ushort, MqttZeroMessageLease> inbound in inboundQos2)
        {
            if (_messageStore.IsEnabled)
            {
                await SaveInboundQos2Async(
                    inbound.Value,
                    session.ClientId,
                    inbound.Key,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
