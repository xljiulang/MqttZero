using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Observers;
using MqttZero.Server.Retained;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private void RegisterSessionDeadlines(MqttZeroSessionState session)
    {
        MqttZeroShardOwnership.EnsureCurrent(
            session.ShardId,
            nameof(RegisterSessionDeadlines),
            _fanoutDiagnostics);
        MqttZeroClientConnection? connection = session.Connection;
        DateTimeOffset? keepAliveDueAt = null;
        if (connection != null && connection.KeepAliveSeconds > 0)
        {
            keepAliveDueAt = connection.LastPacketReceivedAt
                .AddSeconds(connection.KeepAliveSeconds * 1.5);
        }

        DateTimeOffset? sessionExpiresAt = null;
        if (session.DisconnectedAt.HasValue &&
            session.ExpiryInterval > 0)
        {
            sessionExpiresAt = session.DisconnectedAt.Value
                .AddSeconds(session.ExpiryInterval);
        }

        ScheduleSessionTimer(
            session,
            MqttZeroDeadlineKind.KeepAlive,
            keepAliveDueAt);
        ScheduleSessionTimer(
            session,
            MqttZeroDeadlineKind.Will,
            session.WillDueAt);
        ScheduleSessionTimer(
            session,
            MqttZeroDeadlineKind.SessionExpiry,
            sessionExpiresAt);
        RescheduleSessionMessageExpiry(session);
    }

    private async ValueTask RegisterSessionDeadlinesOnShardAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        await RunOnShardAsync(
            session.ShardId,
            () => RegisterSessionDeadlines(session),
            cancellationToken).ConfigureAwait(false);
    }

    private void RemoveSessionDeadlines(MqttZeroSessionState session)
    {
        MqttZeroShardOwnership.EnsureCurrent(
            session.ShardId,
            nameof(RemoveSessionDeadlines),
            _fanoutDiagnostics);
        MqttZeroShardTimerWheel wheel = GetSessionTimerWheel(session);
        wheel.Cancel(session.GetTimerNode(
            MqttZeroDeadlineKind.KeepAlive));
        wheel.Cancel(session.GetTimerNode(
            MqttZeroDeadlineKind.Will));
        wheel.Cancel(session.GetTimerNode(
            MqttZeroDeadlineKind.SessionExpiry));
        wheel.Cancel(session.GetTimerNode(
            MqttZeroDeadlineKind.MessageExpiry));
    }

    private async ValueTask RemoveSessionDeadlinesOnShardAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        await RunOnShardAsync(
            session.ShardId,
            () => RemoveSessionDeadlines(session),
            cancellationToken).ConfigureAwait(false);
    }

    private void ScheduleSessionTimer(
        MqttZeroSessionState session,
        MqttZeroDeadlineKind kind,
        DateTimeOffset? dueAt)
    {
        MqttZeroShardOwnership.EnsureCurrent(
            session.ShardId,
            nameof(ScheduleSessionTimer),
            _fanoutDiagnostics);
        MqttZeroShardTimerWheel wheel = GetSessionTimerWheel(session);
        MqttZeroTimerNode node = session.GetTimerNode(kind);
        if (dueAt.HasValue)
        {
            wheel.ScheduleOrRemove(node, dueAt.Value.UtcTicks);
            return;
        }

        wheel.Cancel(node);
    }

    private void RescheduleSessionMessageExpiry(
        MqttZeroSessionState session)
    {
        if (!ShardSet[session.ShardId].IsCurrent)
        {
            MqttZeroQueuedMessage? head =
                session.PendingMessages.Count == 0
                    ? null
                    : session.PendingMessages[0];
            MqttZeroTimerNode? existing =
                session.GetExistingTimerNode(
                    MqttZeroDeadlineKind.MessageExpiry);
            if (head?.ExpiresAt.HasValue != true &&
                existing?.IsScheduled != true)
            {
                return;
            }

            _ = RescheduleSessionMessageExpiryOnShardAsync(session);
            return;
        }

        RescheduleSessionMessageExpiryCore(session);
    }

    private void RescheduleSessionMessageExpiryCore(
        MqttZeroSessionState session)
    {
        MqttZeroShardTimerWheel wheel = GetSessionTimerWheel(session);
        MqttZeroTimerNode node = session.GetTimerNode(
            MqttZeroDeadlineKind.MessageExpiry);
        MqttZeroQueuedMessage? head =
            session.PendingMessages.Count == 0
                ? null
                : session.PendingMessages[0];
        if (head == null || !head.ExpiresAt.HasValue)
        {
            wheel.Cancel(node);
            return;
        }

        wheel.ScheduleOrRemove(
            node,
            head.ExpiresAt.Value.UtcTicks);
    }

    private async Task RescheduleSessionMessageExpiryOnShardAsync(
        MqttZeroSessionState session)
    {
        try
        {
            await RunOnShardAsync(
                session.ShardId,
                () => RescheduleSessionMessageExpiryCore(session),
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogMaintenanceFailure(
                _logger,
                exception);
        }
    }

    private void ScheduleRetainedDeadline(
        string topic,
        DateTimeOffset? expiresAt)
    {
        int shardId = SessionShardMap.GetShardId(topic);
        MqttZeroShardOwnership.EnsureCurrent(
            shardId,
            nameof(ScheduleRetainedDeadline),
            _fanoutDiagnostics);
        if (!_retainedMessages.TryGetValue(
            topic,
            out MqttZeroRetainedMessage? message))
        {
            return;
        }

        MqttZeroShardTimerWheel wheel =
            ShardSet.GetTimerWheel(shardId);
        MqttZeroTimerNode node =
            message.GetExpiryTimerNode(topic);
        if (expiresAt.HasValue)
        {
            wheel.ScheduleOrRemove(node, expiresAt.Value.UtcTicks);
            return;
        }

        wheel.Cancel(node);
    }

    private async ValueTask ScheduleRetainedDeadlineAsync(
        string topic,
        DateTimeOffset? expiresAt,
        CancellationToken cancellationToken)
    {
        int shardId = SessionShardMap.GetShardId(topic);
        await RunOnShardAsync(
            shardId,
            () => ScheduleRetainedDeadline(topic, expiresAt),
            cancellationToken).ConfigureAwait(false);
    }

    private void CancelRetainedDeadline(
        string topic,
        MqttZeroRetainedMessage message)
    {
        int shardId = SessionShardMap.GetShardId(topic);
        MqttZeroShardOwnership.EnsureCurrent(
            shardId,
            nameof(CancelRetainedDeadline),
            _fanoutDiagnostics);
        ShardSet.GetTimerWheel(shardId).Cancel(
            message.GetExpiryTimerNode(topic));
    }

    private async ValueTask CancelRetainedDeadlineAsync(
        string topic,
        MqttZeroRetainedMessage message,
        CancellationToken cancellationToken)
    {
        int shardId = SessionShardMap.GetShardId(topic);
        await RunOnShardAsync(
            shardId,
            () => CancelRetainedDeadline(topic, message),
            cancellationToken).ConfigureAwait(false);
    }

    private void ScheduleAfterTick(
        MqttZeroTimerNode node,
        DateTimeOffset now)
    {
        int shardId = node.Wheel?.ShardId ?? GetOwnerShardId(node);
        MqttZeroShardOwnership.EnsureCurrent(
            shardId,
            nameof(ScheduleAfterTick),
            _fanoutDiagnostics);
        MqttZeroShardTimerWheel wheel =
            node.Wheel ?? ShardSet.GetTimerWheel(shardId);
        wheel.ScheduleOrRemove(
            node,
            now.Add(_options.KeepAliveCheckInterval).UtcTicks);
    }

    private MqttZeroShardTimerWheel GetSessionTimerWheel(
        MqttZeroSessionState session)
    {
        return ShardSet.GetTimerWheel(session.ShardId);
    }

    private int GetOwnerShardId(MqttZeroTimerNode node)
    {
        return node.Owner switch
        {
            MqttZeroSessionState session => session.ShardId,
            MqttZeroRetainedMessage retained =>
                SessionShardMap.GetShardId(node.Id),
            _ => throw new InvalidOperationException(
                "The timer node owner is not supported.")
        };
    }

    private async ValueTask RunOnShardAsync(
        int shardId,
        Action action,
        CancellationToken cancellationToken)
    {
        MqttZeroEventLoop shard = ShardSet[shardId];
        if (shard.IsCurrent)
        {
            action();
            return;
        }

        await shard.InvokeAsync(
            _ =>
            {
                action();
                return ValueTask.FromResult(true);
            },
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ProcessTimerNodeAsync(
        MqttZeroTimerNode node,
        CancellationToken cancellationToken)
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        try
        {
            switch (node.Kind)
            {
                case MqttZeroDeadlineKind.KeepAlive:
                    await ProcessKeepAliveTimerAsync(
                        node,
                        now,
                        cancellationToken).ConfigureAwait(false);
                    break;
                case MqttZeroDeadlineKind.Will:
                    await ProcessWillTimerAsync(
                        node,
                        now,
                        cancellationToken).ConfigureAwait(false);
                    break;
                case MqttZeroDeadlineKind.SessionExpiry:
                    await ProcessSessionExpiryTimerAsync(
                        node,
                        now,
                        cancellationToken).ConfigureAwait(false);
                    break;
                case MqttZeroDeadlineKind.MessageExpiry:
                    await ProcessMessageExpiryTimerAsync(
                        node,
                        cancellationToken).ConfigureAwait(false);
                    break;
                case MqttZeroDeadlineKind.RetainedExpiry:
                    await ProcessRetainedExpiryTimerAsync(
                        node,
                        now,
                        cancellationToken).ConfigureAwait(false);
                    break;
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogMaintenanceFailure(
                _logger,
                exception);
            if (Volatile.Read(ref _stopping) == 0)
            {
                ScheduleAfterTick(node, now);
            }
        }
    }

    private async ValueTask ProcessKeepAliveTimerAsync(
        MqttZeroTimerNode node,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (node.Owner is not MqttZeroSessionState session)
        {
            return;
        }

        MqttZeroClientConnection? connection = session.Connection;
        if (connection == null || connection.KeepAliveSeconds == 0)
        {
            return;
        }

        bool applyKeepAlive =
            _options.DisconnectClientWhenReadingPayload ||
            !connection.IsReadingIncompletePacket;
        if (!applyKeepAlive)
        {
            ScheduleSessionTimer(
                session,
                MqttZeroDeadlineKind.KeepAlive,
                now.Add(_options.KeepAliveCheckInterval));
            return;
        }

        DateTimeOffset keepAliveDueAt =
            connection.LastPacketReceivedAt.AddSeconds(
                connection.KeepAliveSeconds * 1.5);
        if (keepAliveDueAt > now)
        {
            ScheduleSessionTimer(
                session,
                MqttZeroDeadlineKind.KeepAlive,
                keepAliveDueAt);
            return;
        }

        MqttZeroSessionEgress? pump = session.Egress;
        session.Egress = null;
        await StopEgressAsync(pump).ConfigureAwait(false);
        _ = CloseKeepAliveTimeoutConnectionAsync(
            connection,
            cancellationToken);
    }

    private async ValueTask ProcessWillTimerAsync(
        MqttZeroTimerNode node,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (node.Owner is not MqttZeroSessionState session ||
            !session.WillDueAt.HasValue ||
            session.WillMessage == null)
        {
            return;
        }

        if (session.WillDueAt.Value.UtcTicks != node.DueUtcTicks ||
            session.WillDueAt.Value > now)
        {
            ScheduleSessionTimer(
                session,
                MqttZeroDeadlineKind.Will,
                session.WillDueAt);
            return;
        }

        MqttZeroWillMessage will = session.WillMessage;
        if (!ReferenceEquals(session.WillMessage, will) ||
            !session.WillDueAt.HasValue ||
            session.WillDueAt.Value > _timeProvider.GetUtcNow())
        {
            return;
        }

        await RoutePublishAsync(
            null,
            will.CreatePacket(),
            cancellationToken,
            session.ShardId).ConfigureAwait(false);
        if (ReferenceEquals(session.WillMessage, will))
        {
            session.WillMessage = null;
            session.WillDueAt = null;
        }

        Interlocked.Increment(ref _publishedWillCount);
        if (session.DisconnectedAt.HasValue &&
            session.ExpiryInterval > 0)
        {
            ScheduleSessionTimer(
                session,
                MqttZeroDeadlineKind.SessionExpiry,
                session.DisconnectedAt.Value.AddSeconds(
                    session.ExpiryInterval));
        }
    }

    private async ValueTask ProcessSessionExpiryTimerAsync(
        MqttZeroTimerNode node,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (node.Owner is not MqttZeroSessionState session)
        {
            return;
        }

        if (session.WillDueAt.HasValue)
        {
            ScheduleSessionTimer(
                session,
                MqttZeroDeadlineKind.SessionExpiry,
                now.Add(_options.KeepAliveCheckInterval));
            return;
        }

        DateTimeOffset? expiresAt = session.DisconnectedAt.HasValue &&
            session.ExpiryInterval > 0
            ? session.DisconnectedAt.Value.AddSeconds(
                session.ExpiryInterval)
            : null;
        if (expiresAt.HasValue && expiresAt.Value > now)
        {
            ScheduleSessionTimer(
                session,
                MqttZeroDeadlineKind.SessionExpiry,
                expiresAt);
            return;
        }

        if (!await TryRemoveExpiredSessionAsync(
            session,
            cancellationToken).ConfigureAwait(false))
        {
            ScheduleSessionTimer(
                session,
                MqttZeroDeadlineKind.SessionExpiry,
                now.Add(_options.KeepAliveCheckInterval));
        }
    }

    private async ValueTask ProcessMessageExpiryTimerAsync(
        MqttZeroTimerNode node,
        CancellationToken cancellationToken)
    {
        if (node.Owner is not MqttZeroSessionState session)
        {
            return;
        }

        List<MqttZeroQueuedMessage> expired = [];
        lock (session.SyncRoot)
        {
            session.PendingMessages.RemoveExpired(
                _timeProvider.GetUtcNow(),
                expired);
        }

        for (int index = 0; index < expired.Count; index++)
        {
            MqttZeroQueuedMessage message = expired[index];
            try
            {
                await DeleteMessageAsync(
                    session.ClientId,
                    message.MessageId,
                    cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                lock (session.SyncRoot)
                {
                    for (int restore = expired.Count - 1;
                        restore >= index;
                        restore--)
                    {
                        session.PendingMessages.Prepend(
                            expired[restore]);
                    }
                }

                RescheduleSessionMessageExpiry(session);
                throw;
            }

            await NotifyDroppedAsync(
                null,
                session,
                message.Lease,
                MqttZeroMessageDropReason.Expired,
                cancellationToken).ConfigureAwait(false);
            message.Release();
        }

        RescheduleSessionMessageExpiry(session);
        SignalEgress(session);
    }

    private async ValueTask ProcessRetainedExpiryTimerAsync(
        MqttZeroTimerNode node,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (node.Owner is not MqttZeroRetainedMessage message ||
            !_retainedMessages.TryGetValue(
                node.Id,
                out MqttZeroRetainedMessage? current) ||
            !ReferenceEquals(current, message) ||
            !message.ExpiresAt.HasValue)
        {
            return;
        }

        DateTimeOffset expiresAt = message.ExpiresAt.Value;
        if (expiresAt.UtcTicks != node.DueUtcTicks ||
            expiresAt > now)
        {
            ScheduleRetainedDeadline(
                node.Id,
                expiresAt);
            return;
        }

        try
        {
            await DeleteRetainedAsync(
                node.Id,
                cancellationToken).ConfigureAwait(false);
            RemoveRetainedMessageCore(node.Id);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroRetainedMessageStore.DeleteAsync),
                node.Id,
                exception);
            ScheduleAfterTick(node, now);
        }
    }

    private async ValueTask<bool> TryRemoveExpiredSessionAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        bool removed = false;
        lock (_sessionGate)
        {
            if (_sessions.TryGetValue(
                    session.ClientId,
                    out MqttZeroSessionState? current) &&
                ReferenceEquals(current, session) &&
                IsSessionExpired(session, _timeProvider.GetUtcNow()) &&
                !session.WillDueAt.HasValue)
            {
                _sessions.TryRemove(session.ClientId, out _);
                removed = true;
            }
        }

        if (!removed)
        {
            return true;
        }

        RemoveSessionDeadlines(session);
        await RemoveSessionSubscriptionsFromRouteAsync(session)
            .ConfigureAwait(false);
        MqttZeroSessionEgress? pump = session.Egress;
        session.Egress = null;
        await StopEgressAsync(pump).ConfigureAwait(false);
        try
        {
            await DeleteClientPersistenceAsync(
                session.ClientId,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            lock (_sessionGate)
            {
                _sessions[session.ClientId] = session;
            }

            await AddSessionSubscriptionsToRouteAsync(session)
                .ConfigureAwait(false);
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(DeleteClientPersistenceAsync),
                session.ClientId,
                exception);
            return false;
        }

        ReleaseSessionMessages(session);
        return true;
    }

    private async Task CloseKeepAliveTimeoutConnectionAsync(
        MqttZeroClientConnection connection,
        CancellationToken cancellationToken)
    {
        try
        {
            await connection.CloseAsync(
                MqttReasonCode.KeepAliveTimeout,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogConnectionCleanupFailure(
                _logger,
                exception);
        }
    }
}
