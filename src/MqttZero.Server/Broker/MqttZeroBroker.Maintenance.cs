using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Retained;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements keep-alive, will, and periodic broker maintenance.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async Task RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(_options.KeepAliveCheckInterval, _timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            try
            {
                DateTimeOffset now = _timeProvider.GetUtcNow();
                _keepAliveExpired.Clear();
                _dueWills.Clear();
                _expiredSessions.Clear();
                _expiredRetainedTopics.Clear();

                foreach (MqttZeroSessionState session in _sessions.Values)
                {
                    lock (session.SyncRoot)
                    {
                        if (session.Connection != null && session.Connection.KeepAliveSeconds > 0)
                        {
                            bool applyKeepAlive =
                                _options.DisconnectClientWhenReadingPayload ||
                                !session.Connection.IsReadingIncompletePacket;
                            double secondsWithoutPacket =
                                (now - session.Connection.LastPacketReceivedAt).TotalSeconds;
                            if (applyKeepAlive &&
                                secondsWithoutPacket > session.Connection.KeepAliveSeconds * 1.5)
                            {
                                _keepAliveExpired.Add((
                                    session.Connection,
                                    session.Egress));
                                session.Egress = null;
                            }
                        }

                        if (session.WillDueAt.HasValue && session.WillDueAt.Value <= now)
                        {
                            if (session.WillMessage != null)
                            {
                                _dueWills.Add((session, session.WillMessage));
                            }
                        }

                        if (IsSessionExpired(session, now) && !session.WillDueAt.HasValue)
                        {
                            _expiredSessions.Add(session);
                        }
                    }
                }

                foreach ((
                    MqttZeroClientConnection connection,
                    MqttZeroEgressActor? pump) in _keepAliveExpired)
                {
                    try
                    {
                        await StopEgressAsync(pump).ConfigureAwait(false);
                        await connection.CloseAsync(MqttReasonCode.KeepAliveTimeout, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        MqttZeroBrokerLogging.LogConnectionCleanupFailure(_logger, exception);
                    }
                }

                foreach ((MqttZeroSessionState session, MqttZeroWillMessage will) in _dueWills)
                {
                    using SessionMutationLease mutation = await AcquireSessionMutationAsync(
                        session.ClientId,
                        cancellationToken).ConfigureAwait(false);
                    try
                    {
                        lock (session.SyncRoot)
                        {
                            if (!ReferenceEquals(session.WillMessage, will) ||
                                !session.WillDueAt.HasValue ||
                                session.WillDueAt.Value > _timeProvider.GetUtcNow())
                            {
                                continue;
                            }
                        }

                        await RoutePublishAsync(null, will.CreatePacket(), cancellationToken)
                            .ConfigureAwait(false);
                        lock (session.SyncRoot)
                        {
                            if (ReferenceEquals(session.WillMessage, will))
                            {
                                session.WillMessage = null;
                                session.WillDueAt = null;
                            }
                        }

                        Interlocked.Increment(ref _publishedWillCount);
                    }
                    catch (Exception exception)
                    {
                        MqttZeroBrokerLogging.LogMaintenanceFailure(_logger, exception);
                    }
                }

                foreach (KeyValuePair<string, MqttZeroRetainedMessage> pair in _retainedMessages)
                {
                    if (pair.Value.ExpiresAt.HasValue && pair.Value.ExpiresAt.Value <= now)
                    {
                        _expiredRetainedTopics.Add(pair.Key);
                    }
                }

                foreach (string topic in _expiredRetainedTopics)
                {
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
                    }
                }

                foreach (MqttZeroSessionState session in _expiredSessions)
                {
                    using SessionMutationLease mutation = await AcquireSessionMutationAsync(
                        session.ClientId,
                        cancellationToken).ConfigureAwait(false);
                    bool removed = false;
                    lock (_sessionGate)
                    {
                        if (_sessions.TryGetValue(session.ClientId, out MqttZeroSessionState? current) &&
                            ReferenceEquals(current, session) &&
                            IsSessionExpired(session, _timeProvider.GetUtcNow()) &&
                            !session.WillDueAt.HasValue)
                        {
                            _sessions.TryRemove(session.ClientId, out _);
                            removed = true;
                        }
                    }

                    if (removed)
                    {
                        await RemoveSessionSubscriptionsFromRouteAsync(session)
                            .ConfigureAwait(false);
                        MqttZeroEgressActor? pump;
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
                            _sessions[session.ClientId] = session;
                            await AddSessionSubscriptionsToRouteAsync(session)
                                .ConfigureAwait(false);
                            MqttZeroBrokerLogging.LogPersistenceFailure(
                                _logger,
                                nameof(DeleteClientPersistenceAsync),
                                session.ClientId,
                                exception);
                            continue;
                        }

                        ReleaseSessionMessages(session);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogMaintenanceFailure(_logger, exception);
            }
        }
    }

}
