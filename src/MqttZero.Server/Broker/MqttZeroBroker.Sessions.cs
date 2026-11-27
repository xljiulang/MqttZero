using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// MQTT 5 Broker 主体，按连接、会话、路由、投递与生命周期拆成多个分部文件。
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// 拆离连接。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="cleanDisconnect">clean断开。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask DetachConnectionAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        bool cleanDisconnect)
    {
        MqttZeroShardSet? shards = Volatile.Read(ref _shardSet);
        if (shards == null)
        {
            await DetachConnectionOnShardAsync(
                session,
                connection,
                cleanDisconnect,
                CancellationToken.None).ConfigureAwait(false);
            return;
        }

        MqttZeroEventLoop shard = shards[session.ShardId];
        if (shard.IsCurrent)
        {
            await DetachConnectionOnShardAsync(
                session,
                connection,
                cleanDisconnect,
                CancellationToken.None).ConfigureAwait(false);
            return;
        }

        await shard.InvokeAsync(
            async token =>
            {
                await DetachConnectionOnShardAsync(
                    session,
                    connection,
                    cleanDisconnect,
                    token).ConfigureAwait(false);
                return true;
            },
            CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// 拆离连接On分片。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="cleanDisconnect">clean断开。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask DetachConnectionOnShardAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        bool cleanDisconnect,
        CancellationToken cancellationToken)
    {
        EnsureSessionShard(
            session,
            nameof(DetachConnectionOnShardAsync));
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
                await SaveSessionAsync(
                    session,
                    cancellationToken).ConfigureAwait(false);
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

                ReleaseSessionMessagesCore(session);
                try
                {
                    await DeleteClientPersistenceAsync(
                        session.ClientId,
                        cancellationToken).ConfigureAwait(false);
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
                        cancellationToken).ConfigureAwait(false);
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

                ReleaseSessionMessagesCore(session);
            }
        }
    }

    /// <summary>
    /// 停止出站。
    /// </summary>
    /// <param name="pump">对应的 pump。</param>
    /// <returns>表示异步操作的任务。</returns>
    private static async ValueTask StopEgressAsync(
        MqttZeroSessionEgress? pump)
    {
        if (pump != null)
        {
            await pump.StopAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 判断是否会话Expired。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="now">当前时间。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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

    /// <summary>
    /// 确保会话分片。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="operation">对应的 operation。</param>
    private void EnsureSessionShard(
        MqttZeroSessionState session,
        string operation)
    {
        if (Volatile.Read(ref _shardSet) == null)
        {
            return;
        }

        MqttZeroShardOwnership.EnsureCurrent(
            session.ShardId,
            operation,
            _fanoutDiagnostics);
    }

    /// <summary>
    /// Clamp会话Expiry。
    /// </summary>
    /// <param name="requestedExpiry">对应的 requestedExpiry。</param>
    /// <returns>对应的结果。</returns>
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
