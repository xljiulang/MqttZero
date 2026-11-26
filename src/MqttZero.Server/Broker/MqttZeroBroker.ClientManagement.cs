using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Retained;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Text;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    /// <summary>Tries to get the connected client handle for a client identifier.</summary>
    public bool TryGetClient(
        string clientId,
        out MqttZeroConnectedClient? client)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        if (!_sessions.TryGetValue(clientId, out MqttZeroSessionState? session))
        {
            client = null;
            return false;
        }

        MqttZeroClientConnection? connection;
        lock (session.SyncRoot)
        {
            connection = session.Connection;
        }

        if (connection == null)
        {
            client = null;
            return false;
        }

        client = connection.ConnectedClient;
        return true;
    }

    internal static bool IsConnectionAttached(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        lock (session.SyncRoot)
        {
            return ReferenceEquals(session.Connection, connection);
        }
    }

    internal static MqttZeroClientSnapshot CreateClientSnapshot(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        EnsureAttached(session, connection);
        int subscriptionCount;
        lock (session.SyncRoot)
        {
            subscriptionCount = session.Subscriptions.Count;
        }

        return new MqttZeroClientSnapshot
        {
            ClientId = session.ClientId,
            UserName = connection.ClientInfo.UserName,
            RemoteEndPoint = connection.RemoteEndPoint,
            IsSecure = connection.IsSecure,
            IsWebSocket = connection.IsWebSocket,
            ProtocolVersion = connection.ClientInfo.ProtocolVersion,
            SubscriptionCount = subscriptionCount,
            Statistics = connection.GetStatistics()
        };
    }

    internal ValueTask<IReadOnlyList<MqttZeroSubscribeResult>> SubscribeConnectedClientAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        IReadOnlyList<MqttSubscriptionRequest> subscriptions,
        uint? subscriptionIdentifier,
        CancellationToken cancellationToken)
    {
        return InvokeOnSessionShardAsync(
            session,
            token => SubscribeConnectedClientCoreAsync(
                session,
                connection,
                subscriptions,
                subscriptionIdentifier,
                token),
            cancellationToken);
    }

    private async ValueTask<IReadOnlyList<MqttZeroSubscribeResult>>
        SubscribeConnectedClientCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        IReadOnlyList<MqttSubscriptionRequest> subscriptions,
        uint? subscriptionIdentifier,
        CancellationToken cancellationToken)
    {
        EnsureAttached(session, connection);
        ArgumentNullException.ThrowIfNull(subscriptions);
        List<MqttZeroSubscribeResult> results = new(subscriptions.Count);
        bool changed = false;
        List<MqttZeroSubscription> original;
        lock (session.SyncRoot)
        {
            original = [.. session.Subscriptions];
        }

        List<MqttZeroSubscription> candidate = new(original);
        List<MqttSubscriptionRequest> retainedRequests = [];
        for (int index = 0; index < subscriptions.Count; index++)
        {
            MqttSubscriptionRequest request = subscriptions[index];
            if (!MqttZeroProtocolValidator.IsValidTopicFilter(request.TopicFilter))
            {
                results.Add(new MqttZeroSubscribeResult
                {
                    TopicFilter = Encoding.UTF8.GetString(request.TopicFilter.FirstSpan),
                    QualityOfServiceLevel = request.QualityOfServiceLevel,
                    ReasonCode = MqttReasonCode.TopicFilterInvalid
                });
                continue;
            }

            MqttZeroSubscriptionDecision decision =
                await EvaluateSubscriptionAsync(
                    connection,
                    request,
                    subscriptionIdentifier,
                    candidate,
                    cancellationToken).ConfigureAwait(false);
            if (decision.CloseConnection)
            {
                await connection.CloseAsync(decision.ReasonCode, cancellationToken)
                    .ConfigureAwait(false);
                throw new InvalidOperationException("The subscription closed the connection.");
            }

            bool quotaExceeded =
                decision.Outcome == MqttZeroSubscriptionOutcome.QuotaExceeded;
            results.Add(new MqttZeroSubscribeResult
            {
                TopicFilter = GetTopicFilterText(request.TopicFilter),
                QualityOfServiceLevel = quotaExceeded
                    ? default
                    : request.QualityOfServiceLevel,
                ReasonCode = decision.ReasonCode
            });
            if (decision.Outcome != MqttZeroSubscriptionOutcome.Subscribed)
            {
                continue;
            }

            changed = true;
            if (!ShouldDeliverRetained(request, decision.Existed))
            {
                continue;
            }

            retainedRequests.Add(request);
        }

        if (changed)
        {
            await CommitSubscriptionsAsync(
                session,
                original,
                candidate,
                cancellationToken).ConfigureAwait(false);
        }

        for (int index = 0; index < retainedRequests.Count; index++)
        {
            await DeliverRetainedForSubscriptionAsync(
                session,
                retainedRequests[index],
                subscriptionIdentifier,
                cancellationToken).ConfigureAwait(false);
        }

        return results;
    }

    internal ValueTask<IReadOnlyList<MqttZeroUnsubscribeResult>> UnsubscribeConnectedClientAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        IReadOnlyList<string> topicFilters,
        CancellationToken cancellationToken)
    {
        return InvokeOnSessionShardAsync(
            session,
            token => UnsubscribeConnectedClientCoreAsync(
                session,
                connection,
                topicFilters,
                token),
            cancellationToken);
    }

    private async ValueTask<IReadOnlyList<MqttZeroUnsubscribeResult>>
        UnsubscribeConnectedClientCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        IReadOnlyList<string> topicFilters,
        CancellationToken cancellationToken)
    {
        EnsureAttached(session, connection);
        ArgumentNullException.ThrowIfNull(topicFilters);
        List<MqttZeroUnsubscribeResult> results = new(topicFilters.Count);
        bool changed = false;
        List<MqttZeroSubscription> original;
        lock (session.SyncRoot)
        {
            original = [.. session.Subscriptions];
        }

        List<MqttZeroSubscription> candidate = new(original);
        foreach (string topicFilter in topicFilters)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(topicFilter);
            ReadOnlySequence<byte> encodedFilter =
                new(Encoding.UTF8.GetBytes(topicFilter));
            if (!MqttZeroProtocolValidator.IsValidTopicFilter(encodedFilter))
            {
                results.Add(new MqttZeroUnsubscribeResult
                {
                    TopicFilter = topicFilter,
                    ReasonCode = MqttReasonCode.TopicFilterInvalid
                });
                continue;
            }

            MqttZeroUnsubscriptionDecision decision =
                await EvaluateUnsubscriptionAsync(
                    connection,
                    encodedFilter,
                    candidate,
                    cancellationToken).ConfigureAwait(false);
            if (decision.CloseConnection)
            {
                await connection.CloseAsync(decision.ReasonCode, cancellationToken)
                    .ConfigureAwait(false);
                throw new InvalidOperationException("The unsubscribe closed the connection.");
            }

            changed |= decision.Changed;
            results.Add(new MqttZeroUnsubscribeResult
            {
                TopicFilter = topicFilter,
                ReasonCode = decision.ReasonCode
            });
        }

        if (changed)
        {
            await CommitSubscriptionsAsync(
                session,
                original,
                candidate,
                cancellationToken).ConfigureAwait(false);
        }

        return results;
    }

    internal ValueTask<bool> SendPublishToClientAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken)
    {
        return InvokeOnSessionShardAsync(
            session,
            token => SendPublishToClientCoreAsync(
                session,
                connection,
                message,
                token),
            cancellationToken);
    }

    private async ValueTask<bool> SendPublishToClientCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(message.Topic);
        if (!IsConnectionAttached(session, connection) ||
            State != MqttZeroBrokerState.Running)
        {
            return false;
        }

        ReadOnlySequence<byte> topic = new(Encoding.UTF8.GetBytes(message.Topic));
        if (!MqttZeroProtocolValidator.IsValidTopicName(topic))
        {
            throw new ArgumentException("The MQTT topic name is invalid.", nameof(message));
        }

        MqttPublishProperties properties = message.Properties ?? MqttPublishProperties.Empty;
        if (properties.PayloadFormatIndicator == MqttPayloadFormatIndicator.Utf8 &&
            !MqttZeroProtocolValidator.IsValidUtf8(message.Payload))
        {
            throw new ArgumentException("The MQTT payload is not valid UTF-8.", nameof(message));
        }

        ReadOnlySequence<byte> encodedProperties = MqttPublishPropertiesCodec.Encode(properties);
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            topic,
            encodedProperties,
            message.Payload,
            message.QualityOfServiceLevel,
            message.Retain,
            properties.MessageExpiryInterval,
            null,
            _bufferPool);
        lease.SetExpiresAt(CalculateMessageExpiresAt(properties.MessageExpiryInterval));
        try
        {
            MqttZeroPublishView view = new(
                lease.TopicSequence,
                lease.PropertiesSequence,
                lease.PayloadSequence,
                lease.QualityOfServiceLevel,
                0,
                false,
                lease.RetainFlag,
                lease.MessageExpiryInterval,
                null,
                null);
            if (MqttPublishPacketCodec.GetEncodedLength(view) > _options.MaxPacketSize)
            {
                throw new InvalidOperationException(
                    "The outbound MQTT packet exceeds the broker maximum packet size.");
            }

            await DeliverLeaseAsync(
                session,
                lease,
                message.QualityOfServiceLevel,
                message.Retain,
                ReadOnlyMemory<uint>.Empty,
                null,
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            lease.Release();
        }
    }

    internal ValueTask<bool> DisconnectConnectedClientAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        return InvokeOnSessionShardAsync(
            session,
            token => DisconnectConnectedClientCoreAsync(
                session,
                connection,
                reasonCode,
                token),
            cancellationToken);
    }

    private static async ValueTask<bool> DisconnectConnectedClientCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        MqttZeroEgressActor? pump;
        lock (session.SyncRoot)
        {
            if (!ReferenceEquals(session.Connection, connection))
            {
                return false;
            }

            pump = session.Egress;
            session.Egress = null;
        }

        await StopEgressAsync(pump).ConfigureAwait(false);
        await connection.CloseAsync(reasonCode, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private ValueTask<T> InvokeOnSessionShardAsync<T>(
        MqttZeroSessionState session,
        Func<CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken)
    {
        MqttZeroShardSet? shards = Volatile.Read(ref _shardSet);
        if (shards == null)
        {
            return operation(cancellationToken);
        }

        if (session.ShardId < 0 || session.ShardId >= shards.Count)
        {
            throw new InvalidOperationException(
                "The MQTT session references an invalid event loop shard.");
        }

        return shards[session.ShardId].InvokeAsync(
            operation,
            cancellationToken);
    }

    private static void EnsureAttached(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        lock (session.SyncRoot)
        {
            if (!ReferenceEquals(session.Connection, connection))
            {
                throw new InvalidOperationException("The connected client handle is stale.");
            }
        }
    }
}
