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
        MqttZeroEgressActor? pump;

        lock (session.SyncRoot)
        {
            if (!ReferenceEquals(session.Connection, connection))
            {
                return;
            }

            pump = session.Egress;
            session.Egress = null;
            session.Connection = null;
            session.DisconnectedAt = now;
            uint expiryInterval = session.ExpiryInterval;
            if (cleanDisconnect || Volatile.Read(ref _stopping) != 0)
            {
                session.WillDueAt = null;
                session.WillMessage = null;
            }
            else if (session.WillMessage != null)
            {
                uint delay = session.WillMessage.DelayInterval;
                if (expiryInterval > 0)
                {
                    delay = Math.Min(delay, expiryInterval);
                }

                session.WillDueAt = now.AddSeconds(delay);
            }

            if (expiryInterval == 0 && session.WillDueAt == null)
            {
                removeSession = true;
            }
        }

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
                    _sessions[session.ClientId] = session;
                    await AddSessionSubscriptionsToRouteAsync(session)
                        .ConfigureAwait(false);
                    throw;
                }

                ReleaseSessionMessages(session);
            }
        }
    }

    private static async ValueTask StopEgressAsync(
        MqttZeroEgressActor? pump)
    {
        if (pump != null)
        {
            await pump.StopAsync().ConfigureAwait(false);
        }
    }

    private static bool IsSessionExpired(MqttZeroSessionState session, DateTimeOffset now)
    {
        if (session.Connection != null || !session.DisconnectedAt.HasValue)
        {
            return false;
        }

        return session.ExpiryInterval == 0 ||
            now >= session.DisconnectedAt.Value.AddSeconds(session.ExpiryInterval);
    }

    private int GetInflightLimit(MqttZeroClientConnection? connection)
    {
        if (connection == null)
        {
            return _options.MaxInflight;
        }

        return Math.Min(_options.MaxInflight, connection.ReceiveMaximum);
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
