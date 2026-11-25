using MqttZero.Server.Interceptors;
using MqttZero.Server.Clients;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private async ValueTask RoutePublishLeaseFastAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        bool suppressPublisherAck,
        CancellationToken cancellationToken)
    {
        MqttZeroMessageLease effectiveLease = lease;
        bool ownsEffectiveLease = false;
        if (lease.ExpiresAt.HasValue &&
            lease.ExpiresAt.Value <= _timeProvider.GetUtcNow())
        {
            if (!suppressPublisherAck &&
                lease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
            {
                await senderConnection.SendPubAckAsync(
                    lease.PacketIdentifier,
                    MqttReasonCode.Success,
                    cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        if (_publishInterceptors.Length > 0)
        {
            MqttZeroPublishContext context = MqttZeroPublishContext.Rent();
            context.Topic = new ReadOnlySequence<byte>(lease.Topic);
            context.Payload = new ReadOnlySequence<byte>(lease.Payload);
            if (!MqttPublishPropertiesCodec.TryDecode(
                    lease.Properties.Length == 0
                        ? ReadOnlySequence<byte>.Empty
                        : new ReadOnlySequence<byte>(lease.Properties),
                    out MqttPublishProperties properties,
                    out _,
                    out _))
            {
                context.Release();
                throw new InvalidOperationException(
                    "The MQTT PUBLISH properties are invalid.");
            }

            context.Properties = properties;
            context.QualityOfServiceLevel = lease.QualityOfServiceLevel;
            context.Retain = lease.RetainFlag;
            context.Sender = senderConnection.ConnectedClient;
            try
            {
                foreach (IMqttZeroPublishInterceptor interceptor in _publishInterceptors)
                {
                    MqttZeroInterceptionResult result = await interceptor
                        .InterceptAsync(context, cancellationToken)
                        .ConfigureAwait(false);
                    if (result != MqttZeroInterceptionResult.Continue)
                    {
                        if (result == MqttZeroInterceptionResult.CloseConnection)
                        {
                            await senderConnection.CloseAsync(
                                context.ResultReasonCode == MqttReasonCode.Success
                                    ? MqttReasonCode.NotAuthorized
                                    : context.ResultReasonCode,
                                cancellationToken).ConfigureAwait(false);
                        }
                        else if (!suppressPublisherAck &&
                            lease.QualityOfServiceLevel ==
                            MqttQualityOfServiceLevel.AtLeastOnce)
                        {
                            await senderConnection.SendPubAckAsync(
                                lease.PacketIdentifier,
                                context.ResultReasonCode,
                                cancellationToken).ConfigureAwait(false);
                        }

                        return;
                    }
                }

                effectiveLease = MqttZeroMessageLease.CopyFrom(
                    context.Topic,
                    MqttPublishPropertiesCodec.Encode(context.Properties),
                    context.Payload,
                    context.QualityOfServiceLevel,
                    context.Retain,
                    context.Properties.MessageExpiryInterval,
                    lease.SubscriptionIdentifier,
                    _bufferPool);
                effectiveLease.SetExpiresAt(
                    context.Properties.MessageExpiryInterval ==
                        lease.MessageExpiryInterval
                        ? lease.ExpiresAt
                        : CalculateMessageExpiresAt(
                            context.Properties.MessageExpiryInterval));
                effectiveLease.PacketIdentifier = lease.PacketIdentifier;
                ownsEffectiveLease = true;
            }
            finally
            {
                context.Release();
            }
        }

        MqttZeroRouteScope? deliveries = null;
        try
        {
            if (effectiveLease.RetainFlag)
            {
                StoreRetainedMessageInMemory(effectiveLease);
            }

            MqttZeroSubscriptionRouteSnapshot routeSnapshot =
                Volatile.Read(ref _subscriptionRouteSnapshot);
            deliveries = MqttZeroRouteSelector.Select(
                routeSnapshot,
                effectiveLease.Topic.Span,
                sender,
                _routeScopes);
            for (int index = 0; index < deliveries.Count; index++)
            {
                MqttZeroDeliveryCandidate delivery = deliveries[index];
                MqttQualityOfServiceLevel deliveryQos =
                    (MqttQualityOfServiceLevel)Math.Min(
                        (byte)effectiveLease.QualityOfServiceLevel,
                        (byte)delivery.QualityOfServiceLevel);
                await DeliverLeaseFastAsync(
                    delivery.Session,
                    effectiveLease,
                    deliveryQos,
                    delivery.Subscription.RetainAsPublished && effectiveLease.RetainFlag,
                    delivery.SubscriptionIdentifiers,
                    cancellationToken).ConfigureAwait(false);
            }

            if (!suppressPublisherAck &&
                effectiveLease.QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtLeastOnce)
            {
                await senderConnection.SendPubAckAsync(
                    effectiveLease.PacketIdentifier,
                    MqttReasonCode.Success,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (deliveries != null)
            {
                for (int index = 0; index < deliveries.Count; index++)
                {
                    deliveries[index].Session.Egress?.Signal();
                }
            }

            deliveries?.Dispose();
            if (ownsEffectiveLease)
            {
                effectiveLease.Release();
            }
        }
    }

    private ValueTask DeliverLeaseFastAsync(
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qos,
        bool retain,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        CancellationToken cancellationToken)
    {
        DateTimeOffset? expiresAt = lease.ExpiresAt;
        if (expiresAt.HasValue &&
            expiresAt.Value <= _timeProvider.GetUtcNow())
        {
            return ValueTask.CompletedTask;
        }

        MqttZeroClientConnection? connection;
        lock (session.SyncRoot)
        {
            connection = session.Connection;
        }

        if (qos == MqttQualityOfServiceLevel.AtMostOnce &&
            connection == null &&
            !_options.StoreQos0InQueue)
        {
            return ValueTask.CompletedTask;
        }

        int encodedLength = 0;
        if (connection != null)
        {
            encodedLength = connection.GetPublishEncodedLength(
                lease,
                qos,
                retain,
                subscriptionIdentifiers);
            if (encodedLength > connection.MaximumPacketSize)
            {
                return ValueTask.CompletedTask;
            }
        }

        MqttZeroQueuedMessage message = MqttZeroQueuedMessage.RentWithIdentifiers(
            lease,
            qos,
            retain,
            subscriptionIdentifiers,
            expiresAt);
        message.SetMessageId(GetNextMessageId());
        MqttZeroQueuedMessage? evicted = null;
        bool enqueued = false;
        try
        {
            lock (session.SyncRoot)
            {
                if (qos == MqttQualityOfServiceLevel.AtMostOnce &&
                    session.Connection == null &&
                    !_options.StoreQos0InQueue)
                {
                    return ValueTask.CompletedTask;
                }

                MqttZeroClientConnection? currentConnection = session.Connection;
                if (currentConnection != null &&
                    ReferenceEquals(currentConnection, connection))
                {
                    message.SetEncodedLength(
                        encodedLength,
                        currentConnection.MaximumPacketSize);
                }

                evicted = EnqueueMessage(session, message);
                enqueued = true;
            }
        }
        finally
        {
            if (!enqueued)
            {
                message.Release();
            }
        }

        evicted?.Release();
        return ValueTask.CompletedTask;
    }
}
