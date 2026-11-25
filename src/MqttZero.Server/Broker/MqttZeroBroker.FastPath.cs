using MqttZero.Server.Interceptors;
using MqttZero.Server.Clients;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
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

        try
        {
            effectiveLease.EnsureOwned();
            if (effectiveLease.RetainFlag)
            {
                StoreRetainedMessageInMemory(effectiveLease);
            }

            MqttZeroFanoutDispatcher dispatcher =
                Volatile.Read(ref _fanoutDispatcher) ??
                throw new InvalidOperationException(
                    "The MQTTZero fanout dispatcher is not running.");
            MqttZeroRoutePlannerLanes lanes =
                Volatile.Read(ref _routePlannerLanes) ??
                throw new InvalidOperationException(
                    "The MQTTZero route planner is not running.");
            MqttZeroShardSet shards = Volatile.Read(ref _shardSet) ??
                throw new InvalidOperationException(
                    "The MQTTZero event loop shards are not running.");
            int publisherShardId = sender.ShardId;
            if (effectiveLease.QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtMostOnce)
            {
                MqttZeroRoutePlanBufferPool qosZeroPlanBuffers =
                    Volatile.Read(ref _qosZeroPlanBuffers) ??
                    throw new InvalidOperationException(
                        "The MQTTZero QoS 0 planner buffers are not running.");
                MqttZeroRoutePlanBuffer buffer =
                    qosZeroPlanBuffers.Rent();
                MqttZeroRoutePlan plan;
                try
                {
                    plan = MqttZeroRoutePlanner.Plan(
                        _routeEpochs.Current,
                        effectiveLease.Topic.Span,
                        sender,
                        _sharedGroupCursors,
                        shards.Count,
                        buffer);
                }
                catch
                {
                    qosZeroPlanBuffers.Return(buffer);
                    throw;
                }

                if (plan.TotalDeliveryCount == 0)
                {
                    qosZeroPlanBuffers.Return(buffer);
                }
                else
                {
                    await dispatcher.DispatchQosZeroAsync(
                        plan,
                        buffer,
                        effectiveLease,
                        senderConnection,
                        publisherShardId,
                        qosZeroPlanBuffers.ReturnCallback,
                        cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                    _routeEpochs.Current,
                    effectiveLease.Topic.Span,
                    sender,
                    _sharedGroupCursors,
                    shards.Count,
                    lanes[publisherShardId]);
                if (plan.TotalDeliveryCount > 0)
                {
                    if (effectiveLease.QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.AtLeastOnce)
                    {
                        await dispatcher.DispatchScheduledAsync(
                            plan,
                            lanes[publisherShardId],
                            effectiveLease,
                            senderConnection,
                            publisherShardId,
                            cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await dispatcher.DispatchAsync(
                            plan,
                            lanes[publisherShardId],
                            effectiveLease,
                            senderConnection,
                            publisherShardId,
                            cancellationToken).ConfigureAwait(false);
                    }
                }
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
        CancellationToken cancellationToken,
        bool copySubscriptionIdentifiers = false)
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

        MqttZeroQueuedMessage message = copySubscriptionIdentifiers
            ? MqttZeroQueuedMessage.RentWithCopiedIdentifiers(
                lease,
                qos,
                retain,
                subscriptionIdentifiers,
                expiresAt)
            : MqttZeroQueuedMessage.RentWithIdentifiers(
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
        if (enqueued)
        {
            SignalEgress(session);
        }

        return ValueTask.CompletedTask;
    }
}
