using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Retained;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// MQTT 5 Broker 主体，按连接、会话、路由、投递与生命周期拆成多个分部文件。
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// 持久化。
    /// </summary>
    /// <param name="operation">对应的 operation。</param>
    /// <param name="key">键。</param>
    /// <param name="state">状态。</param>
    /// <param name="callback">回调。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 恢复Persistence。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RestorePersistenceAsync(CancellationToken cancellationToken)
    {
        Dictionary<string, MqttZeroSessionState> stagedSessions = new(StringComparer.Ordinal);
        Dictionary<string, MqttZeroRetainedMessage> stagedRetainedMessages = new(StringComparer.Ordinal);
        MqttZeroRetainedTopicTrie stagedRetainedTrie = MqttZeroRetainedTopicTrie.Empty;
        List<(MqttZeroSessionState Session, MqttZeroMessageLease Lease, long MessageId)> inboundDispatches = [];
        bool inboundDispatchesReleased = true;
        long stagedRetainedBytes = 0;
        bool committed = false;
        try
        {
            await RestoreSessionsAsync(stagedSessions, cancellationToken).ConfigureAwait(false);
            inboundDispatchesReleased = await RestoreMessagesAsync(stagedSessions, inboundDispatches, cancellationToken).ConfigureAwait(false);
            (stagedRetainedTrie, stagedRetainedBytes) = await RestoreRetainedMessagesAsync(stagedRetainedMessages, cancellationToken).ConfigureAwait(false);
            await CommitRestoredStateAsync(stagedSessions, stagedRetainedMessages, stagedRetainedTrie, stagedRetainedBytes, cancellationToken).ConfigureAwait(false);
            await DispatchRestoredInboundAsync(inboundDispatches, cancellationToken).ConfigureAwait(false);
            inboundDispatchesReleased = true;
            committed = true;
        }
        catch (Exception exception)
        {
            ResetRestoredState();
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
                await ReleaseStagedStateAsync(
                    stagedSessions,
                    stagedRetainedMessages,
                    inboundDispatches,
                    inboundDispatchesReleased,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// 从会话存储恢复会话与订阅。
    /// </summary>
    /// <param name="stagedSessions">暂存会话字典。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RestoreSessionsAsync(
        Dictionary<string, MqttZeroSessionState> stagedSessions,
        CancellationToken cancellationToken)
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
                await DeleteClientPersistenceAsync(persisted.ClientId, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (stagedSessions.Count >= _options.MaxSessions)
            {
                await DeleteClientPersistenceAsync(persisted.ClientId, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (persisted.Will != null && !IsValidPersistedWill(persisted.Will))
            {
                await DeleteClientPersistenceAsync(persisted.ClientId, cancellationToken).ConfigureAwait(false);
                continue;
            }

            MqttZeroSessionState session = CreateRestoredSession(persisted);
            if (!TryRestoreSubscriptions(session, persisted.Subscriptions))
            {
                await DeleteClientPersistenceAsync(persisted.ClientId, cancellationToken).ConfigureAwait(false);
                continue;
            }

            stagedSessions.Add(session.ClientId, session);
        }
    }

    /// <summary>
    /// 按持久化记录构造会话状态，包含遗嘱与遗嘱到期时间。
    /// </summary>
    /// <param name="persisted">持久化会话。</param>
    /// <returns>恢复出的会话状态。</returns>
    private MqttZeroSessionState CreateRestoredSession(MqttZeroPersistedSession persisted)
    {
        MqttZeroSessionState session = CreateSessionState(persisted.ClientId);
        MqttZeroWillMessage? restoredWill = persisted.Will == null ? null : new MqttZeroWillMessage(persisted.Will);
        DateTimeOffset? willDueAt = null;
        if (persisted.DisconnectedAt.HasValue && restoredWill != null)
        {
            uint delay = restoredWill.DelayInterval;
            if (persisted.ExpiryInterval > 0)
            {
                delay = Math.Min(delay, persisted.ExpiryInterval);
            }

            willDueAt = persisted.DisconnectedAt.Value.AddSeconds(delay);
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
        return session;
    }

    /// <summary>
    /// 把持久化订阅恢复到会话；任一订阅非法即判定整条会话无效。
    /// </summary>
    /// <param name="session">目标会话。</param>
    /// <param name="subscriptions">持久化订阅集合。</param>
    /// <returns>为 <see langword="true"/> 表示全部订阅有效且已恢复。</returns>
    private bool TryRestoreSubscriptions(
        MqttZeroSessionState session,
        IReadOnlyList<MqttZeroPersistedSubscription> subscriptions)
    {
        foreach (MqttZeroPersistedSubscription subscription in subscriptions)
        {
            if (!IsValidSubscription(subscription))
            {
                return false;
            }

            if (session.Subscriptions.Count >= _options.MaxSubscriptionsPerSession)
            {
                return false;
            }

            session.Subscriptions.Add(
                new MqttZeroSubscription(
                    subscription.TopicFilter.ToArray(),
                    subscription.QualityOfServiceLevel,
                    subscription.NoLocal,
                    subscription.RetainAsPublished,
                    subscription.SubscriptionIdentifier));
        }

        return true;
    }

    /// <summary>
    /// 从消息存储恢复在途与排队消息，并清理孤儿消息。
    /// </summary>
    /// <param name="stagedSessions">暂存会话字典。</param>
    /// <param name="inboundDispatches">待分发的入站消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>为 <see langword="true"/> 表示没有遗留未释放的入站消息。</returns>
    private async ValueTask<bool> RestoreMessagesAsync(
        Dictionary<string, MqttZeroSessionState> stagedSessions,
        List<(MqttZeroSessionState Session, MqttZeroMessageLease Lease, long MessageId)> inboundDispatches,
        CancellationToken cancellationToken)
    {
        bool released = true;
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
                await DeletePersistedMessageAsync(persisted, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (persisted.Kind == MqttZeroPersistedMessageKind.InboundQos2)
            {
                await RestoreInboundQos2MessageAsync(persisted, session, cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (persisted.Kind == MqttZeroPersistedMessageKind.InboundDispatch)
            {
                RestoreInboundDispatchMessage(persisted, session, inboundDispatches);
                released = false;
                continue;
            }

            await RestoreQueuedMessageAsync(persisted, session, cancellationToken).ConfigureAwait(false);
        }

        if (_messageStore.IsEnabled)
        {
            foreach (string clientId in orphanClients)
            {
                await DeleteClientMessagesAsync(clientId, cancellationToken).ConfigureAwait(false);
            }
        }

        return released;
    }

    /// <summary>
    /// 恢复一条入站 QoS 2 消息；重复或超出上限时直接从存储删除。
    /// </summary>
    /// <param name="persisted">持久化消息。</param>
    /// <param name="session">目标会话。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RestoreInboundQos2MessageAsync(
        MqttZeroPersistedMessage persisted,
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        if (session.InboundQos2Messages.ContainsKey(persisted.PacketIdentifier) ||
            session.InboundQos2Messages.Count >= _options.MaxAwaitingRel)
        {
            await DeletePersistedMessageAsync(persisted, cancellationToken).ConfigureAwait(false);
            return;
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
    }

    /// <summary>
    /// 恢复一条待分发的入站消息，稍后按时间顺序重新路由。
    /// </summary>
    /// <param name="persisted">持久化消息。</param>
    /// <param name="session">目标会话。</param>
    /// <param name="inboundDispatches">待分发的入站消息。</param>
    private void RestoreInboundDispatchMessage(
        MqttZeroPersistedMessage persisted,
        MqttZeroSessionState session,
        List<(MqttZeroSessionState Session, MqttZeroMessageLease Lease, long MessageId)> inboundDispatches)
    {
        MqttZeroMessageLease dispatchLease = MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>(persisted.Topic),
            NormalizePersistedMessageProperties(persisted.Properties),
            new ReadOnlySequence<byte>(persisted.Payload),
            persisted.QualityOfServiceLevel,
            persisted.Retain,
            null,
            null,
            _bufferPool);
        dispatchLease.SetExpiresAt(persisted.ExpiresAt);
        dispatchLease.PacketIdentifier = persisted.PacketIdentifier;
        inboundDispatches.Add((session, dispatchLease, persisted.MessageId));
    }

    /// <summary>
    /// 恢复一条在途或排队消息；重复或超出上限时直接从存储删除。
    /// </summary>
    /// <param name="persisted">持久化消息。</param>
    /// <param name="session">目标会话。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RestoreQueuedMessageAsync(
        MqttZeroPersistedMessage persisted,
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        if ((persisted.Kind == MqttZeroPersistedMessageKind.Outbound &&
                (session.InflightMessages.ContainsKey(persisted.PacketIdentifier) ||
                    session.InflightMessages.Count >= _options.MaxInflight)) ||
            (persisted.Kind == MqttZeroPersistedMessageKind.Queued &&
                session.PendingMessages.Count >= _options.MaxMessageQueueLength))
        {
            await DeletePersistedMessageAsync(persisted, cancellationToken).ConfigureAwait(false);
            return;
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
                MqttZeroPersistedDeliveryState.AwaitPubComp => MqttZeroOutboundState.AwaitPubComp,
                MqttZeroPersistedDeliveryState.AwaitPubRec => MqttZeroOutboundState.AwaitPubRec,
                _ => MqttZeroOutboundState.AwaitPubAck
            };
            message.Restore(persisted.MessageId, persisted.PacketIdentifier, state);
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

    /// <summary>
    /// 从保留消息存储恢复保留消息与前缀树。
    /// </summary>
    /// <param name="stagedRetainedMessages">暂存保留消息字典。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>恢复后的前缀树与保留字节数。</returns>
    private async ValueTask<(MqttZeroRetainedTopicTrie Trie, long Bytes)> RestoreRetainedMessagesAsync(
        Dictionary<string, MqttZeroRetainedMessage> stagedRetainedMessages,
        CancellationToken cancellationToken)
    {
        MqttZeroRetainedTopicTrie stagedRetainedTrie = MqttZeroRetainedTopicTrie.Empty;
        long stagedRetainedBytes = 0;
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

        return (stagedRetainedTrie, stagedRetainedBytes);
    }

    /// <summary>
    /// 把暂存的会话与保留消息发布为可用的 Broker 状态。
    /// </summary>
    /// <param name="stagedSessions">暂存会话字典。</param>
    /// <param name="stagedRetainedMessages">暂存保留消息字典。</param>
    /// <param name="stagedRetainedTrie">恢复后的前缀树。</param>
    /// <param name="stagedRetainedBytes">保留字节数。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask CommitRestoredStateAsync(
        Dictionary<string, MqttZeroSessionState> stagedSessions,
        Dictionary<string, MqttZeroRetainedMessage> stagedRetainedMessages,
        MqttZeroRetainedTopicTrie stagedRetainedTrie,
        long stagedRetainedBytes,
        CancellationToken cancellationToken)
    {
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
    }

    /// <summary>
    /// 按时间顺序分发恢复出来的入站消息。
    /// </summary>
    /// <param name="inboundDispatches">待分发的入站消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask DispatchRestoredInboundAsync(
        List<(MqttZeroSessionState Session, MqttZeroMessageLease Lease, long MessageId)> inboundDispatches,
        CancellationToken cancellationToken)
    {
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
    }

    /// <summary>
    /// 把恢复失败后的会话、保留消息与路由快照清空。
    /// </summary>
    private void ResetRestoredState()
    {
            lock (_sessionGate)
            {
                _sessions.Clear();
                _retainedMessages.Clear();
                Volatile.Write(
                    ref _retainedTopicTrie,
                    MqttZeroRetainedTopicTrie.Empty);
                _retainedBytes = 0;
                _routeEpochs.ResetToEmpty();
                _sharedGroupCursors.PruneTo([]);
            }

            _sharedGroupReferences.Clear();
    }

    /// <summary>
    /// 释放恢复失败时仍被暂存的租约与消息。
    /// </summary>
    /// <param name="stagedSessions">暂存会话字典。</param>
    /// <param name="stagedRetainedMessages">暂存保留消息字典。</param>
    /// <param name="inboundDispatches">待分发的入站消息。</param>
    /// <param name="inboundDispatchesReleased">入站消息是否已释放。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ReleaseStagedStateAsync(
        Dictionary<string, MqttZeroSessionState> stagedSessions,
        Dictionary<string, MqttZeroRetainedMessage> stagedRetainedMessages,
        List<(MqttZeroSessionState Session, MqttZeroMessageLease Lease, long MessageId)> inboundDispatches,
        bool inboundDispatchesReleased,
        CancellationToken cancellationToken)
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

    /// <summary>
    /// 删除Persisted消息。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 判断是否ValidPersisted遗嘱。
    /// </summary>
    /// <param name="will">遗嘱。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool IsValidPersistedWill(MqttZeroPersistedWill will)
    {
        return IsValidPublishTopic(will.Topic) &&
            IsValidQualityOfService(will.QualityOfServiceLevel);
    }

    /// <summary>
    /// 判断是否Valid订阅。
    /// </summary>
    /// <param name="subscription">订阅。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool IsValidSubscription(
        MqttZeroPersistedSubscription subscription)
    {
        return MqttZeroProtocolValidator.IsValidTopicFilter(
                new ReadOnlySequence<byte>(subscription.TopicFilter)) &&
            IsValidQualityOfService(subscription.QualityOfServiceLevel) &&
            (!subscription.SubscriptionIdentifier.HasValue ||
                subscription.SubscriptionIdentifier.Value != 0);
    }

    /// <summary>
    /// 判断是否ValidPersisted消息。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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

    /// <summary>
    /// 判断是否Valid保留消息。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool IsValidRetainedMessage(
        MqttZeroPersistedRetainedMessage message)
    {
        return IsValidPublishTopic(message.Topic) &&
            IsValidQualityOfService(message.QualityOfServiceLevel);
    }

    /// <summary>
    /// 判断是否Valid发布主题。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool IsValidPublishTopic(ReadOnlyMemory<byte> topic)
    {
        return MqttZeroProtocolValidator.IsValidTopicName(
            new ReadOnlySequence<byte>(topic));
    }

    /// <summary>
    /// 判断是否Valid服务质量Service。
    /// </summary>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool IsValidQualityOfService(
        MqttQualityOfServiceLevel qualityOfServiceLevel)
    {
        return (byte)qualityOfServiceLevel <=
            (byte)MqttQualityOfServiceLevel.ExactlyOnce;
    }

    /// <summary>
    /// 保存会话。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask SaveSessionAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        await SaveSessionAsync(
            session,
            null,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 保存会话订阅。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="subscriptions">订阅。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 保存会话。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="candidateSubscriptions">candidate订阅。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 保存保留。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 保存Queued。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 保存出站。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 保存入站Qos2。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 保存入站Dispatch。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="sender">对应的 sender。</param>
    /// <param name="messageId">消息Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 删除入站Dispatch。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="messageId">消息Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// TransitionQueuedTo出站。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 替换Queued。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="replacedMessageId">replaced消息Id。</param>
    /// <param name="message">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 删除消息。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="messageId">消息Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 删除入站Qos2。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 删除客户端消息。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 删除会话。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 删除保留。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 创建Persisted消息。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="kind">种类。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 创建入站Persisted消息。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 创建入站DispatchPersisted消息。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="messageId">消息Id。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 比较入站DispatchOrder。
    /// </summary>
    /// <param name="left">对应的 left。</param>
    /// <param name="right">对应的 right。</param>
    /// <returns>计算得到的数值。</returns>
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

    /// <summary>
    /// NormalizePersisted消息属性。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <returns>只读字节序列。</returns>
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

    /// <summary>
    /// 尝试NormalizePersisted消息属性。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="normalized">对应的 normalized。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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

    /// <summary>
    /// 计算消息ExpiresAt。
    /// </summary>
    /// <param name="expiryInterval">expiry间隔。</param>
    /// <returns>对应的结果。</returns>
    private DateTimeOffset? CalculateMessageExpiresAt(uint? expiryInterval)
    {
        return expiryInterval.HasValue
            ? _timeProvider.GetUtcNow().AddSeconds(expiryInterval.Value)
            : null;
    }

    /// <summary>
    /// 获取Remaining消息Expiry。
    /// </summary>
    /// <param name="expiresAt">对应的 expiresAt。</param>
    /// <param name="now">当前时间。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 创建Persisted遗嘱。
    /// </summary>
    /// <param name="will">遗嘱。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// 删除客户端Persistence。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask DeleteClientPersistenceAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        await DeleteClientMessagesAsync(clientId, cancellationToken).ConfigureAwait(false);
        await DeleteSessionAsync(clientId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 恢复会话Persistence。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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