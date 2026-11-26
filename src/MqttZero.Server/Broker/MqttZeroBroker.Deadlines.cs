using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Retained;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private void RegisterSessionDeadlines(MqttZeroSessionState session)
    {
        string clientId = session.ClientId;
        DateTimeOffset? keepAliveDueAt = null;
        DateTimeOffset? willDueAt;
        DateTimeOffset? sessionExpiresAt = null;
        lock (session.SyncRoot)
        {
            MqttZeroClientConnection? connection = session.Connection;
            if (connection != null && connection.KeepAliveSeconds > 0)
            {
                keepAliveDueAt = connection.LastPacketReceivedAt
                    .AddSeconds(connection.KeepAliveSeconds * 1.5);
            }

            willDueAt = session.WillDueAt;
            if (session.DisconnectedAt.HasValue &&
                session.ExpiryInterval > 0)
            {
                sessionExpiresAt = session.DisconnectedAt.Value
                    .AddSeconds(session.ExpiryInterval);
            }
        }

        ScheduleOrRemove(
            MqttZeroDeadlineKind.KeepAlive,
            clientId,
            keepAliveDueAt);
        ScheduleOrRemove(
            MqttZeroDeadlineKind.Will,
            clientId,
            willDueAt);
        ScheduleOrRemove(
            MqttZeroDeadlineKind.SessionExpiry,
            clientId,
            sessionExpiresAt);
    }

    private void RemoveSessionDeadlines(string clientId)
    {
        _deadlines.Remove(new MqttZeroDeadlineKey(
            MqttZeroDeadlineKind.KeepAlive,
            clientId));
        _deadlines.Remove(new MqttZeroDeadlineKey(
            MqttZeroDeadlineKind.Will,
            clientId));
        _deadlines.Remove(new MqttZeroDeadlineKey(
            MqttZeroDeadlineKind.SessionExpiry,
            clientId));
    }

    private void ScheduleOrRemove(
        MqttZeroDeadlineKind kind,
        string id,
        DateTimeOffset? dueAt)
    {
        if (dueAt.HasValue)
        {
            _deadlines.Schedule(
                new MqttZeroDeadlineKey(kind, id),
                dueAt.Value.UtcTicks);
            return;
        }

        _deadlines.Remove(new MqttZeroDeadlineKey(kind, id));
    }

    private void ScheduleRetainedDeadline(
        string topic,
        DateTimeOffset? expiresAt)
    {
        ScheduleOrRemove(
            MqttZeroDeadlineKind.RetainedExpiry,
            topic,
            expiresAt);
    }

    private void RemoveRetainedDeadline(string topic)
    {
        _deadlines.Remove(new MqttZeroDeadlineKey(
            MqttZeroDeadlineKind.RetainedExpiry,
            topic));
    }

    private void ScheduleAfterTick(
        MqttZeroDeadlineKind kind,
        string id,
        DateTimeOffset now)
    {
        _deadlines.Schedule(
            new MqttZeroDeadlineKey(kind, id),
            now.Add(_options.KeepAliveCheckInterval).UtcTicks);
    }

    private MqttZeroSessionState? FindSession(string clientId)
    {
        lock (_sessionGate)
        {
            return _sessions.TryGetValue(
                clientId,
                out MqttZeroSessionState? session)
                ? session
                : null;
        }
    }

    private async ValueTask ProcessDeadlineAsync(
        MqttZeroDeadline deadline,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        switch (deadline.Key.Kind)
        {
            case MqttZeroDeadlineKind.KeepAlive:
                await ProcessKeepAliveDeadlineAsync(
                    deadline,
                    now,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttZeroDeadlineKind.Will:
                await ProcessWillDeadlineAsync(
                    deadline,
                    now,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttZeroDeadlineKind.SessionExpiry:
                await ProcessSessionExpiryDeadlineAsync(
                    deadline,
                    now,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttZeroDeadlineKind.RetainedExpiry:
                await ProcessRetainedExpiryDeadlineAsync(
                    deadline,
                    now,
                    cancellationToken).ConfigureAwait(false);
                break;
        }
    }

    private async ValueTask ProcessKeepAliveDeadlineAsync(
        MqttZeroDeadline deadline,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        MqttZeroSessionState? session = FindSession(deadline.Key.Id);
        if (session == null)
        {
            return;
        }

        MqttZeroClientConnection? connection;
        MqttZeroSessionEgress? pump = null;
        DateTimeOffset rescheduleAt = default;
        bool reschedule = false;
        lock (session.SyncRoot)
        {
            connection = session.Connection;
            if (connection == null || connection.KeepAliveSeconds == 0)
            {
                return;
            }

            bool applyKeepAlive =
                _options.DisconnectClientWhenReadingPayload ||
                !connection.IsReadingIncompletePacket;
            if (!applyKeepAlive)
            {
                reschedule = true;
                rescheduleAt = now.Add(_options.KeepAliveCheckInterval);
            }
            else
            {
                DateTimeOffset keepAliveDueAt =
                    connection.LastPacketReceivedAt.AddSeconds(
                        connection.KeepAliveSeconds * 1.5);
                if (keepAliveDueAt > now)
                {
                    reschedule = true;
                    rescheduleAt = keepAliveDueAt;
                }
                else
                {
                    pump = session.Egress;
                    session.Egress = null;
                }
            }
        }

        if (reschedule)
        {
            _deadlines.Schedule(deadline.Key, rescheduleAt.UtcTicks);
            return;
        }

        if (connection == null)
        {
            return;
        }

        try
        {
            await StopEgressAsync(pump).ConfigureAwait(false);
            await connection.CloseAsync(
                MqttReasonCode.KeepAliveTimeout,
                cancellationToken).ConfigureAwait(false);
            _deadlines.Remove(deadline.Key);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogConnectionCleanupFailure(
                _logger,
                exception);
        }
    }

    private async ValueTask ProcessWillDeadlineAsync(
        MqttZeroDeadline deadline,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        MqttZeroSessionState? session = FindSession(deadline.Key.Id);
        if (session == null)
        {
            return;
        }

        MqttZeroWillMessage? will;
        lock (session.SyncRoot)
        {
            if (!session.WillDueAt.HasValue ||
                session.WillMessage == null)
            {
                return;
            }

            if (session.WillDueAt.Value.UtcTicks != deadline.DueTicks ||
                session.WillDueAt.Value > now)
            {
                _deadlines.Schedule(
                    deadline.Key,
                    session.WillDueAt.Value.UtcTicks);
                return;
            }

            will = session.WillMessage;
        }

        using SessionMutationLease mutation =
            await AcquireSessionMutationAsync(
                session.ClientId,
                cancellationToken).ConfigureAwait(false);
        DateTimeOffset? sessionExpiresAt = null;
        try
        {
            lock (session.SyncRoot)
            {
                if (!ReferenceEquals(session.WillMessage, will) ||
                    !session.WillDueAt.HasValue ||
                    session.WillDueAt.Value > _timeProvider.GetUtcNow())
                {
                    return;
                }
            }

            await RoutePublishAsync(
                null,
                will.CreatePacket(),
                cancellationToken).ConfigureAwait(false);
            lock (session.SyncRoot)
            {
                if (ReferenceEquals(session.WillMessage, will))
                {
                    session.WillMessage = null;
                    session.WillDueAt = null;
                }

                if (session.DisconnectedAt.HasValue &&
                    session.ExpiryInterval > 0)
                {
                    sessionExpiresAt = session.DisconnectedAt.Value
                        .AddSeconds(session.ExpiryInterval);
                }
            }

            Interlocked.Increment(ref _publishedWillCount);
            if (sessionExpiresAt.HasValue)
            {
                _deadlines.Schedule(
                    new MqttZeroDeadlineKey(
                        MqttZeroDeadlineKind.SessionExpiry,
                        session.ClientId),
                    sessionExpiresAt.Value.UtcTicks);
            }
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogMaintenanceFailure(
                _logger,
                exception);
            ScheduleAfterTick(
                MqttZeroDeadlineKind.Will,
                session.ClientId,
                now);
        }
    }

    private async ValueTask ProcessSessionExpiryDeadlineAsync(
        MqttZeroDeadline deadline,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        MqttZeroSessionState? session = FindSession(deadline.Key.Id);
        if (session == null)
        {
            return;
        }

        DateTimeOffset? willDueAt;
        DateTimeOffset? expiresAt;
        lock (session.SyncRoot)
        {
            willDueAt = session.WillDueAt;
            expiresAt = session.DisconnectedAt.HasValue &&
                session.ExpiryInterval > 0
                ? session.DisconnectedAt.Value.AddSeconds(
                    session.ExpiryInterval)
                : null;
        }

        if (willDueAt.HasValue)
        {
            ScheduleAfterTick(
                MqttZeroDeadlineKind.SessionExpiry,
                session.ClientId,
                now);
            return;
        }

        if (expiresAt.HasValue && expiresAt.Value > now)
        {
            _deadlines.Schedule(
                deadline.Key,
                expiresAt.Value.UtcTicks);
            return;
        }

        if (!await TryRemoveExpiredSessionAsync(
            session,
            cancellationToken).ConfigureAwait(false))
        {
            ScheduleAfterTick(
                MqttZeroDeadlineKind.SessionExpiry,
                session.ClientId,
                now);
        }
    }

    private async ValueTask ProcessRetainedExpiryDeadlineAsync(
        MqttZeroDeadline deadline,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        string topic = deadline.Key.Id;
        if (!_retainedMessages.TryGetValue(
            topic,
            out MqttZeroRetainedMessage? message) ||
            !message.ExpiresAt.HasValue)
        {
            return;
        }

        DateTimeOffset expiresAt = message.ExpiresAt.Value;
        if (expiresAt.UtcTicks != deadline.DueTicks ||
            expiresAt > now)
        {
            _deadlines.Schedule(deadline.Key, expiresAt.UtcTicks);
            return;
        }

        try
        {
            await DeleteRetainedAsync(topic, cancellationToken)
                .ConfigureAwait(false);
            RemoveRetainedMessage(topic);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroRetainedMessageStore.DeleteAsync),
                topic,
                exception);
            ScheduleAfterTick(
                MqttZeroDeadlineKind.RetainedExpiry,
                topic,
                now);
        }
    }

    private async ValueTask<bool> TryRemoveExpiredSessionAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        using SessionMutationLease mutation =
            await AcquireSessionMutationAsync(
                session.ClientId,
                cancellationToken).ConfigureAwait(false);
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

        RemoveSessionDeadlines(session.ClientId);
        await RemoveSessionSubscriptionsFromRouteAsync(session)
            .ConfigureAwait(false);
        MqttZeroSessionEgress? pump;
        lock (session.SyncRoot)
        {
            pump = session.Egress;
            session.Egress = null;
        }

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
}
