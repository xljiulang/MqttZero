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
    private async ValueTask PersistAsync<TState>(
        string operation,
        string key,
        TState state,
        MqttZeroStoreOperation<TState> callback,
        CancellationToken cancellationToken)
    {
        try
        {
            await callback(state, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                operation,
                key,
                exception);
            throw;
        }
    }

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
                MqttZeroWillMessage? restoredWill =
                    persisted.Will == null
                    ? null
                    : new MqttZeroWillMessage(persisted.Will);
                DateTimeOffset? willDueAt = null;
                if (persisted.DisconnectedAt.HasValue &&
                    restoredWill != null)
                {
                    uint delay = restoredWill.DelayInterval;
                    if (persisted.ExpiryInterval > 0)
                    {
                        delay = Math.Min(
                            delay,
                            persisted.ExpiryInterval);
                    }

                    willDueAt =
                        persisted.DisconnectedAt.Value.AddSeconds(
                            delay);
                }

                session.UpdateLifecycle(
                    current => current with
                    {
                        CleanStart = persisted.CleanStart,
                        ExpiryInterval = persisted.ExpiryInterval,
                        DisconnectedAt = persisted.DisconnectedAt,
                        WillMessage = restoredWill,
                        WillDueAt = willDueAt
                    });
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

                if (!TryNormalizePersistedMessageProperties(
                        persisted.Properties,
                        out ReadOnlySequence<byte> retainedProperties))
                {
                    MqttZeroBrokerLogging.LogInvalidRetainedProperties(
                        _logger,
                        topic);
                    await DeleteRetainedAsync(topic, cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    new ReadOnlySequence<byte>(persisted.Topic),
                    retainedProperties,
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

            await ResetRouteEpochFromSessionsAsync()
                .ConfigureAwait(false);
            foreach (MqttZeroSessionState restoredSession in _sessions.Values)
            {
                await RegisterSessionDeadlinesOnShardAsync(
                    restoredSession,
                    cancellationToken).ConfigureAwait(false);
            }

            foreach (KeyValuePair<string, MqttZeroRetainedMessage> pair in
                _retainedMessages)
            {
                await ScheduleRetainedDeadlineAsync(
                    pair.Key,
                    pair.Value.ExpiresAt,
                    cancellationToken).ConfigureAwait(false);
            }

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
                _sharedGroupCursors.PruneTo([]);
            }

            _sharedGroupReferences.Clear();
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
                    await ReleaseSessionMessagesOnShardAsync(
                        session,
                        cancellationToken).ConfigureAwait(false);
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

        await PersistAsync(
            nameof(DeletePersistedMessageAsync),
            message.ClientId,
            (Store: _messageStore, Message: message),
            DeleteAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask DeleteAsync(
            (IMqttZeroMessageStore Store, MqttZeroPersistedMessage Message) state,
            CancellationToken token)
        {
            if (state.Message.Kind == MqttZeroPersistedMessageKind.InboundQos2)
            {
                return state.Store.DeleteInboundQos2Async(
                    state.Message.ClientId,
                    state.Message.PacketIdentifier,
                    token);
            }

            return state.Store.DeleteMessageAsync(
                state.Message.ClientId,
                state.Message.MessageId,
                token);
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
        MqttZeroSessionLifecycleSnapshot lifecycle =
            session.Lifecycle;
        MqttZeroPersistedWill? will =
            CreatePersistedWill(lifecycle.WillMessage);
        IReadOnlyList<MqttZeroSubscription> source =
            candidateSubscriptions ??
            session.Subscriptions.Snapshot();
        for (int index = 0; index < source.Count; index++)
        {
            MqttZeroSubscription subscription = source[index];
            subscriptions.Add(new MqttZeroPersistedSubscription
            {
                TopicFilter = subscription.TopicFilter,
                QualityOfServiceLevel =
                    subscription.QualityOfServiceLevel,
                NoLocal = subscription.NoLocal,
                RetainAsPublished =
                    subscription.RetainAsPublished,
                SubscriptionIdentifier =
                    subscription.SubscriptionIdentifier
            });
        }

        MqttZeroPersistedSession persisted = new()
        {
            ClientId = session.ClientId,
            CleanStart = lifecycle.CleanStart,
            ExpiryInterval = lifecycle.ExpiryInterval,
            DisconnectedAt = lifecycle.DisconnectedAt,
            Will = will,
            Subscriptions = subscriptions
        };
        await PersistAsync(
            nameof(IMqttZeroSessionStore.SaveAsync),
            session.ClientId,
            (Store: _sessionStore, Session: persisted),
            SaveAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask SaveAsync(
            (IMqttZeroSessionStore Store, MqttZeroPersistedSession Session) state,
            CancellationToken token)
        {
            return state.Store.SaveAsync(state.Session, token);
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

        MqttZeroPersistedRetainedMessage persisted = new()
        {
            Topic = lease.Topic,
            Properties = lease.Properties,
            Payload = lease.Payload,
            QualityOfServiceLevel = lease.QualityOfServiceLevel,
            ExpiresAt = expiresAt
        };
        await PersistAsync(
            nameof(IMqttZeroRetainedMessageStore.SaveAsync),
            Encoding.UTF8.GetString(lease.Topic.Span),
            (Store: _retainedMessageStore, Message: persisted),
            SaveAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask SaveAsync(
            (IMqttZeroRetainedMessageStore Store, MqttZeroPersistedRetainedMessage Message) state,
            CancellationToken token)
        {
            return state.Store.SaveAsync(state.Message, token);
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

        MqttZeroPersistedMessage persisted = CreatePersistedMessage(
            message,
            clientId,
            MqttZeroPersistedMessageKind.Queued);
        await PersistAsync(
            nameof(IMqttZeroMessageStore.SaveQueuedAsync),
            clientId,
            (Store: _messageStore, Message: persisted, ClientId: clientId),
            SaveAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask SaveAsync(
            (
                IMqttZeroMessageStore Store,
                MqttZeroPersistedMessage Message,
                string ClientId) state,
            CancellationToken token)
        {
            return state.Store.SaveQueuedAsync(state.Message, token);
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

        MqttZeroPersistedMessage persisted = CreatePersistedMessage(
            message,
            clientId,
            MqttZeroPersistedMessageKind.Outbound);
        await PersistAsync(
            nameof(IMqttZeroMessageStore.SaveOutboundAsync),
            clientId,
            (Store: _messageStore, Message: persisted),
            SaveAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask SaveAsync(
            (IMqttZeroMessageStore Store, MqttZeroPersistedMessage Message) state,
            CancellationToken token)
        {
            return state.Store.SaveOutboundAsync(state.Message, token);
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

        MqttZeroPersistedMessage persisted =
            CreateInboundPersistedMessage(lease, clientId, packetIdentifier);
        await PersistAsync(
            nameof(IMqttZeroMessageStore.SaveInboundQos2Async),
            clientId,
            (Store: _messageStore, Message: persisted),
            SaveAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask SaveAsync(
            (IMqttZeroMessageStore Store, MqttZeroPersistedMessage Message) state,
            CancellationToken token)
        {
            return state.Store.SaveInboundQos2Async(state.Message, token);
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

        MqttZeroPersistedMessage persisted =
            CreateInboundDispatchPersistedMessage(
                lease,
                sender.ClientId,
                messageId);
        await PersistAsync(
            nameof(IMqttZeroMessageStore.SaveInboundDispatchAsync),
            sender.ClientId,
            (Store: _messageStore, Message: persisted),
            SaveAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask SaveAsync(
            (IMqttZeroMessageStore Store, MqttZeroPersistedMessage Message) state,
            CancellationToken token)
        {
            return state.Store.SaveInboundDispatchAsync(state.Message, token);
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

        await PersistAsync(
            nameof(IMqttZeroMessageStore.DeleteInboundDispatchAsync),
            clientId,
            (Store: _messageStore, ClientId: clientId, MessageId: messageId),
            DeleteAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask DeleteAsync(
            (
                IMqttZeroMessageStore Store,
                string ClientId,
                long MessageId) state,
            CancellationToken token)
        {
            return state.Store.DeleteInboundDispatchAsync(
                state.ClientId,
                state.MessageId,
                token);
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

        MqttZeroPersistedMessage persisted = CreatePersistedMessage(
            message,
            clientId,
            MqttZeroPersistedMessageKind.Outbound);
        await PersistAsync(
            nameof(IMqttZeroMessageStore.TransitionQueuedToOutboundAsync),
            clientId,
            (
                Store: _messageStore,
                ClientId: clientId,
                MessageId: message.MessageId,
                Message: persisted),
            TransitionAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask TransitionAsync(
            (
                IMqttZeroMessageStore Store,
                string ClientId,
                long MessageId,
                MqttZeroPersistedMessage Message) state,
            CancellationToken token)
        {
            return state.Store.TransitionQueuedToOutboundAsync(
                state.ClientId,
                state.MessageId,
                state.Message,
                token);
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

        MqttZeroPersistedMessage persisted = CreatePersistedMessage(
            message,
            clientId,
            MqttZeroPersistedMessageKind.Queued);
        await PersistAsync(
            nameof(IMqttZeroMessageStore.ReplaceQueuedAsync),
            clientId,
            (
                Store: _messageStore,
                ClientId: clientId,
                ReplacedMessageId: replacedMessageId,
                Message: persisted),
            ReplaceAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask ReplaceAsync(
            (
                IMqttZeroMessageStore Store,
                string ClientId,
                long ReplacedMessageId,
                MqttZeroPersistedMessage Message) state,
            CancellationToken token)
        {
            return state.Store.ReplaceQueuedAsync(
                state.ClientId,
                state.ReplacedMessageId,
                state.Message,
                token);
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

        await PersistAsync(
            nameof(IMqttZeroMessageStore.DeleteMessageAsync),
            clientId,
            (Store: _messageStore, ClientId: clientId, MessageId: messageId),
            DeleteAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask DeleteAsync(
            (
                IMqttZeroMessageStore Store,
                string ClientId,
                long MessageId) state,
            CancellationToken token)
        {
            return state.Store.DeleteMessageAsync(
                state.ClientId,
                state.MessageId,
                token);
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

        await PersistAsync(
            nameof(IMqttZeroMessageStore.DeleteInboundQos2Async),
            clientId,
            (
                Store: _messageStore,
                ClientId: clientId,
                PacketIdentifier: packetIdentifier),
            DeleteAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask DeleteAsync(
            (
                IMqttZeroMessageStore Store,
                string ClientId,
                ushort PacketIdentifier) state,
            CancellationToken token)
        {
            return state.Store.DeleteInboundQos2Async(
                state.ClientId,
                state.PacketIdentifier,
                token);
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

        await PersistAsync(
            nameof(IMqttZeroMessageStore.DeleteByClientAsync),
            clientId,
            (Store: _messageStore, ClientId: clientId),
            DeleteAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask DeleteAsync(
            (IMqttZeroMessageStore Store, string ClientId) state,
            CancellationToken token)
        {
            return state.Store.DeleteByClientAsync(state.ClientId, token);
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

        await PersistAsync(
            nameof(IMqttZeroSessionStore.DeleteAsync),
            clientId,
            (Store: _sessionStore, ClientId: clientId),
            DeleteAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask DeleteAsync(
            (IMqttZeroSessionStore Store, string ClientId) state,
            CancellationToken token)
        {
            return state.Store.DeleteAsync(state.ClientId, token);
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

        await PersistAsync(
            nameof(IMqttZeroRetainedMessageStore.DeleteAsync),
            topic,
            (Store: _retainedMessageStore, Topic: topic),
            DeleteAsync,
            cancellationToken).ConfigureAwait(false);

        static ValueTask DeleteAsync(
            (IMqttZeroRetainedMessageStore Store, string Topic) state,
            CancellationToken token)
        {
            return state.Store.DeleteAsync(state.Topic, token);
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
        if (!TryNormalizePersistedMessageProperties(
                properties,
                out ReadOnlySequence<byte> normalized))
        {
            throw new InvalidOperationException(
                "The persisted MQTT PUBLISH properties are invalid.");
        }

        return normalized;
    }

    private static bool TryNormalizePersistedMessageProperties(
        ReadOnlyMemory<byte> properties,
        out ReadOnlySequence<byte> normalized)
    {
        if (properties.IsEmpty)
        {
            normalized = ReadOnlySequence<byte>.Empty;
            return true;
        }

        if (!MqttPublishPropertiesCodec.TryDecode(
                new ReadOnlySequence<byte>(properties),
                out MqttPublishProperties publishProperties,
                out _,
                out _))
        {
            normalized = ReadOnlySequence<byte>.Empty;
            return false;
        }

        normalized = MqttPublishPropertiesCodec.Encode(publishProperties);
        return true;
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
        MqttZeroQueuedMessage[] pending =
            [.. session.PendingMessages];
        MqttZeroQueuedMessage[] inflight =
            [.. session.InflightMessages.Values];
        KeyValuePair<ushort, MqttZeroMessageLease>[] inboundQos2 =
            [.. session.InboundQos2Messages];

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
