using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements session detachment and session resource cleanup.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async ValueTask DetachConnectionAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        bool cleanDisconnect)
    {
        using SessionMutationLease mutation = await AcquireSessionMutationAsync(
            session.ClientId,
            CancellationToken.None).ConfigureAwait(false);
        DateTimeOffset now = _timeProvider.GetUtcNow();
        bool removeSession = false;
        MqttZeroSessionLifecycleSnapshot lifecycle =
            session.Lifecycle;
        if (!ReferenceEquals(
                lifecycle.Connection,
                connection))
        {
            return;
        }

        MqttZeroSessionEgress? pump = session.TakeEgress();
        uint expiryInterval = lifecycle.ExpiryInterval;
        DateTimeOffset? willDueAt = null;
        MqttZeroWillMessage? willMessage =
            lifecycle.WillMessage;
        if (cleanDisconnect || Volatile.Read(ref _stopping) != 0)
        {
            willMessage = null;
        }
        else if (willMessage != null)
        {
            uint delay = willMessage.DelayInterval;
            if (expiryInterval > 0)
            {
                delay = Math.Min(delay, expiryInterval);
            }

            willDueAt = now.AddSeconds(delay);
        }

        session.UpdateLifecycle(
            _ => lifecycle with
            {
                Connection = null,
                DisconnectedAt = now,
                WillDueAt = willDueAt,
                WillMessage = willMessage
            });
        if (expiryInterval == 0 && willDueAt == null)
        {
            removeSession = true;
        }

        await RegisterSessionDeadlinesOnShardAsync(
            session,
            CancellationToken.None).ConfigureAwait(false);
        if (pump != null)
        {
            connection.Abort();
            await StopEgressAsync(pump).ConfigureAwait(false);
        }

        if (!removeSession && _sessionStore.IsEnabled)
        {
            try
            {
                await SaveSessionAsync(session, CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                bool sessionRemoved = false;
                lock (_sessionGate)
                {
                    if (_sessions.TryGetValue(session.ClientId, out MqttZeroSessionState? current) &&
                        ReferenceEquals(current, session))
                    {
                        _sessions.TryRemove(session.ClientId, out _);
                        sessionRemoved = true;
                    }
                }

                if (sessionRemoved)
                {
                    await RemoveSessionSubscriptionsFromRouteAsync(session)
                        .ConfigureAwait(false);
                }

                ReleaseSessionMessages(session);
                try
                {
                    await DeleteClientPersistenceAsync(
                        session.ClientId,
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    MqttZeroBrokerLogging.LogPersistenceFailure(
                        _logger,
                        nameof(DeleteClientPersistenceAsync),
                        session.ClientId,
                        exception);
                }

                throw;
            }
        }

        if (removeSession)
        {
            bool removed = false;
            lock (_sessionGate)
            {
                if (_sessions.TryGetValue(session.ClientId, out MqttZeroSessionState? current) &&
                    ReferenceEquals(current, session))
                {
                    _sessions.TryRemove(session.ClientId, out _);
                    removed = true;
                }
            }

            if (removed)
            {
                await RemoveSessionSubscriptionsFromRouteAsync(session)
                    .ConfigureAwait(false);
                try
                {
                    await DeleteClientPersistenceAsync(
                        session.ClientId,
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    lock (_sessionGate)
                    {
                        _sessions[session.ClientId] = session;
                    }

                    await AddSessionSubscriptionsToRouteAsync(session)
                        .ConfigureAwait(false);
                    throw;
                }

                ReleaseSessionMessages(session);
            }
        }
    }

    private static async ValueTask StopEgressAsync(
        MqttZeroSessionEgress? pump)
    {
        if (pump != null)
        {
            await pump.StopAsync().ConfigureAwait(false);
        }
    }

    private static bool IsSessionExpired(MqttZeroSessionState session, DateTimeOffset now)
    {
        MqttZeroSessionLifecycleSnapshot lifecycle =
            session.Lifecycle;
        if (lifecycle.Connection != null ||
            !lifecycle.DisconnectedAt.HasValue)
        {
            return false;
        }

        return lifecycle.ExpiryInterval == 0 ||
            now >= lifecycle.DisconnectedAt.Value.AddSeconds(
                lifecycle.ExpiryInterval);
    }

    private uint ClampSessionExpiry(uint requestedExpiry)
    {
        if (!_options.MaxSessionExpiryInterval.HasValue)
        {
            return requestedExpiry;
        }

        uint maximumExpiry = (uint)Math.Clamp(
            _options.MaxSessionExpiryInterval.Value.TotalSeconds,
            0,
            uint.MaxValue);
        return Math.Min(requestedExpiry, maximumExpiry);
    }

}
