using MqttZero.Server.Interceptors;
using MqttZero.Server.Clients;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements publish routing, topic aliases, and subscription snapshots.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async ValueTask<bool> AcceptInboundQos2Async(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        bool duplicate = false;
        bool receiveMaximumExceeded = false;
        lock (session.SyncRoot)
        {
            if (session.InboundQos2Messages.ContainsKey(packetIdentifier))
            {
                duplicate = true;
            }
            else if (session.InboundQos2Messages.Count >= _options.MaxAwaitingRel)
            {
                receiveMaximumExceeded = true;
            }
        }

        if (duplicate)
        {
            await connection.SendPubRecAsync(
                packetIdentifier,
                MqttReasonCode.Success,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (receiveMaximumExceeded)
        {
            await connection.SendPubRecAsync(
                packetIdentifier,
                MqttReasonCode.ReceiveMaximumExceeded,
                cancellationToken).ConfigureAwait(false);
            await connection.CloseAsync(
                MqttReasonCode.ReceiveMaximumExceeded,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        MqttZeroPreparedPublish prepared = await PreparePublishForDispatchAsync(
            session,
            connection,
            lease,
            cancellationToken).ConfigureAwait(false);
        if (prepared.Result == MqttZeroInterceptionResult.CloseConnection)
        {
            await connection.CloseAsync(
                prepared.ReasonCode == MqttReasonCode.Success
                    ? MqttReasonCode.NotAuthorized
                    : prepared.ReasonCode,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (prepared.Result != MqttZeroInterceptionResult.Continue ||
            prepared.Lease == null)
        {
            await connection.SendPubRecAsync(
                packetIdentifier,
                prepared.ReasonCode,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        MqttZeroMessageLease storedLease = prepared.Lease;
        bool inserted = false;
        lock (session.SyncRoot)
        {
            if (!session.InboundQos2Messages.ContainsKey(packetIdentifier) &&
                session.InboundQos2Messages.Count < _options.MaxAwaitingRel)
            {
                session.InboundQos2Messages[packetIdentifier] = storedLease;
                session.RoutedInboundQos2Messages.Remove(packetIdentifier);
                inserted = true;
            }
        }

        if (!inserted)
        {
            if (!prepared.UsesSourceLease)
            {
                storedLease.Release();
            }

            await connection.SendPubRecAsync(
                packetIdentifier,
                MqttReasonCode.Success,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (_messageStore.IsEnabled)
        {
            try
            {
                await SaveInboundQos2Async(
                    storedLease,
                    session.ClientId,
                    packetIdentifier,
                    cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                lock (session.SyncRoot)
                {
                    if (session.InboundQos2Messages.TryGetValue(
                            packetIdentifier,
                            out MqttZeroMessageLease? stored) &&
                        ReferenceEquals(stored, storedLease))
                    {
                        session.InboundQos2Messages.Remove(packetIdentifier);
                    }
                }

                if (!prepared.UsesSourceLease)
                {
                    storedLease.Release();
                }

                throw;
            }
        }

        if (!prepared.UsesSourceLease)
        {
            lease.Release();
        }

        await connection.SendPubRecAsync(
            packetIdentifier,
            MqttReasonCode.Success,
            cancellationToken).ConfigureAwait(false);
        return true;
    }

    private async ValueTask RoutePublishAsync(
        MqttZeroSessionState? sender,
        MqttPublishPacket packet,
        CancellationToken cancellationToken)
    {
        if (sender != null && packet.SubscriptionIdentifiers.Count > 0)
        {
            MqttZeroClientConnection? invalidConnection;
            lock (sender.SyncRoot)
            {
                invalidConnection = sender.Connection;
            }

            if (invalidConnection != null)
            {
                await invalidConnection.CloseAsync(
                    MqttReasonCode.ProtocolError,
                    cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        MqttZeroConnectedClient? senderClient = null;
        if (sender != null)
        {
            lock (sender.SyncRoot)
            {
                senderClient = sender.Connection?.ConnectedClient;
            }
        }

        MqttZeroPublishContext context = new()
        {
            Topic = packet.TopicName,
            Payload = packet.Payload,
            Properties = packet.Properties,
            QualityOfServiceLevel = packet.QualityOfServiceLevel,
            Retain = packet.Retain,
            Sender = senderClient
        };

        if (_publishInterceptors.Length > 0)
        {
            foreach (IMqttZeroPublishInterceptor interceptor in _publishInterceptors)
            {
                MqttZeroInterceptionResult result = await interceptor
                    .InterceptAsync(context, cancellationToken)
                    .ConfigureAwait(false);
                if (result != MqttZeroInterceptionResult.Continue)
                {
                    return;
                }
            }
        }

        packet.TopicName = context.Topic;
        packet.Payload = context.Payload;
        packet.Properties = context.Properties;
        packet.QualityOfServiceLevel = context.QualityOfServiceLevel;
        packet.Retain = context.Retain;

        ReadOnlySequence<byte> encodedProperties =
            MqttPublishPropertiesCodec.Encode(packet.Properties);
        MqttZeroMessageLease lease = MqttZeroMessageLease.BorrowFrom(
            packet.TopicName,
            encodedProperties,
            packet.Payload,
            packet.QualityOfServiceLevel,
            packet.Retain,
            packet.Properties.MessageExpiryInterval,
            null,
            _bufferPool);
        lease.SetExpiresAt(CalculateMessageExpiresAt(packet.Properties.MessageExpiryInterval));
        lease.PacketIdentifier = packet.PacketIdentifier;
        try
        {
            if (lease.RetainFlag)
            {
                await StoreRetainedMessageAsync(lease, cancellationToken).ConfigureAwait(false);
            }

            int deliveryCount = await RouteToSubscribersAsync(
                sender,
                null,
                lease,
                sender != null &&
                    lease.QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.AtLeastOnce &&
                    !_messageStore.IsEnabled,
                cancellationToken).ConfigureAwait(false);
            if (deliveryCount == 0 && _deliveryObservers.Length > 0)
            {
                await NotifyNotConsumedAsync(null, lease, cancellationToken).ConfigureAwait(false);
            }

            if (sender != null &&
                lease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
            {
                MqttZeroClientConnection? senderConnection;
                lock (sender.SyncRoot)
                {
                    senderConnection = sender.Connection;
                }

                if (senderConnection != null)
                {
                    await senderConnection.SendPubAckAsync(
                        lease.PacketIdentifier,
                        MqttReasonCode.Success,
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            lease.Release();
        }
    }

    internal MqttZeroMessageLease CreateMessageLease(
        MqttZeroClientConnection connection,
        MqttZeroPublishView view)
    {
        if (!TryCreateMessageLease(
                connection,
                view,
                out MqttZeroMessageLease? lease,
                out MqttReasonCode reasonCode))
        {
            throw new InvalidOperationException(
                $"The inbound MQTT PUBLISH is invalid: {reasonCode}.");
        }

        return lease!;
    }

    internal bool TryCreateMessageLease(
        MqttZeroClientConnection connection,
        MqttZeroPublishView view,
        out MqttZeroMessageLease? lease,
        out MqttReasonCode reasonCode)
    {
        lease = null;
        reasonCode = MqttReasonCode.Success;
        if (!MqttPublishPropertiesCodec.TryDecode(
                view.Properties,
                out MqttPublishProperties properties,
                out IReadOnlyList<uint> subscriptionIdentifiers,
                out _))
        {
            reasonCode = MqttReasonCode.ProtocolError;
            return false;
        }

        if (subscriptionIdentifiers.Count > 0)
        {
            reasonCode = MqttReasonCode.ProtocolError;
            return false;
        }

        if (properties.PayloadFormatIndicator == MqttPayloadFormatIndicator.Utf8 &&
            !MqttZeroProtocolValidator.IsValidUtf8(view.Payload))
        {
            reasonCode = MqttReasonCode.PayloadFormatInvalid;
            return false;
        }

        ReadOnlySequence<byte> topic = view.Topic;
        if (view.TopicAlias.HasValue)
        {
            ushort alias = view.TopicAlias.Value;
            if (alias == 0 || alias > _options.TopicAliasMaximum)
            {
                reasonCode = MqttReasonCode.TopicAliasInvalid;
                return false;
            }

            if (!topic.IsEmpty)
            {
                connection.SetInboundTopicAlias(alias, topic);
            }
            else if (connection.TryGetInboundTopicAlias(alias, out ReadOnlyMemory<byte> mappedTopic))
            {
                topic = new ReadOnlySequence<byte>(mappedTopic);
            }
            else
            {
                reasonCode = MqttReasonCode.TopicAliasInvalid;
                return false;
            }
        }
        else if (topic.IsEmpty)
        {
            reasonCode = MqttReasonCode.ProtocolError;
            return false;
        }

        ReadOnlySequence<byte> encodedProperties =
            MqttPublishPropertiesCodec.Encode(properties);
        lease = MqttZeroMessageLease.BorrowFrom(
            topic,
            encodedProperties,
            view.Payload,
            view.QualityOfServiceLevel,
            view.Retain,
            properties.MessageExpiryInterval,
            null,
            _bufferPool);
        lease.SetExpiresAt(CalculateMessageExpiresAt(properties.MessageExpiryInterval));
        lease.PacketIdentifier = view.PacketIdentifier;
        return true;
    }

    internal ValueTask RoutePublishLeaseAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken,
        bool suppressPublisherAck = false)
    {
        if (!_sessionStore.IsEnabled &&
            !_messageStore.IsEnabled &&
            !_retainedMessageStore.IsEnabled &&
            _enqueueInterceptors.Length == 0 &&
            _deliveryObservers.Length == 0)
        {
            return RoutePublishLeaseFastAsync(
                sender,
                senderConnection,
                lease,
                suppressPublisherAck,
                cancellationToken);
        }

        return RoutePublishLeaseCoreAsync(
            sender,
            senderConnection,
            lease,
            null,
            cancellationToken);
    }

    internal async ValueTask<MqttZeroPreparedPublish> PreparePublishForDispatchAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        if (lease.ExpiresAt.HasValue &&
            lease.ExpiresAt.Value <= _timeProvider.GetUtcNow())
        {
            return new MqttZeroPreparedPublish(
                null,
                MqttZeroInterceptionResult.Consume,
                MqttReasonCode.Success,
                false);
        }

        if (_publishInterceptors.Length == 0)
        {
            lease.EnsureOwned();
            return new MqttZeroPreparedPublish(
                lease,
                MqttZeroInterceptionResult.Continue,
                MqttReasonCode.Success,
                true);
        }

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
                    return new MqttZeroPreparedPublish(
                        null,
                        result,
                        context.ResultReasonCode,
                        false);
                }
            }

            MqttZeroMessageLease effectiveLease = MqttZeroMessageLease.CopyFrom(
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
            return new MqttZeroPreparedPublish(
                effectiveLease,
                MqttZeroInterceptionResult.Continue,
                MqttReasonCode.Success,
                false);
        }
        finally
        {
            context.Release();
        }
    }

    internal async ValueTask RoutePreparedLeaseAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection? senderConnection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        if (lease.RetainFlag)
        {
            await StoreRetainedMessageAsync(
                lease,
                cancellationToken).ConfigureAwait(false);
        }

        int deliveryCount = await RouteToSubscribersAsync(
            sender,
            senderConnection,
            lease,
            scheduleWithoutWaiting: false,
            cancellationToken).ConfigureAwait(false);
        if (deliveryCount == 0 && _deliveryObservers.Length > 0)
        {
            await NotifyNotConsumedAsync(
                senderConnection?.ConnectedClient,
                lease,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask RoutePublishLeaseCoreAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        MqttReasonCode[]? resultHolder,
        CancellationToken cancellationToken)
    {
        MqttZeroMessageLease effectiveLease = lease;
        bool ownsEffectiveLease = false;
        if (lease.ExpiresAt.HasValue &&
            lease.ExpiresAt.Value <= _timeProvider.GetUtcNow())
        {
            if (lease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
            {
                await senderConnection.SendPubAckAsync(
                    lease.PacketIdentifier,
                    MqttReasonCode.Success,
                    cancellationToken).ConfigureAwait(false);
            }

            if (resultHolder != null)
            {
                resultHolder[0] = MqttReasonCode.Success;
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
                        else if (lease.QualityOfServiceLevel ==
                            MqttQualityOfServiceLevel.AtLeastOnce)
                        {
                            await senderConnection.SendPubAckAsync(
                                lease.PacketIdentifier,
                                context.ResultReasonCode,
                                cancellationToken).ConfigureAwait(false);
                        }

                        if (resultHolder != null)
                        {
                            resultHolder[0] = context.ResultReasonCode;
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
            if (effectiveLease.RetainFlag)
            {
                await StoreRetainedMessageAsync(
                    effectiveLease,
                    cancellationToken).ConfigureAwait(false);
            }

            int deliveryCount = await RouteToSubscribersAsync(
                sender,
                senderConnection,
                effectiveLease,
                effectiveLease.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce &&
                    !_messageStore.IsEnabled,
                cancellationToken).ConfigureAwait(false);
            if (deliveryCount == 0 && _deliveryObservers.Length > 0)
            {
                await NotifyNotConsumedAsync(
                    senderConnection.ConnectedClient,
                    effectiveLease,
                    cancellationToken).ConfigureAwait(false);
            }
            if (effectiveLease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
            {
                await senderConnection.SendPubAckAsync(
                    effectiveLease.PacketIdentifier,
                    MqttReasonCode.Success,
                    cancellationToken).ConfigureAwait(false);
            }

            if (resultHolder != null)
            {
                resultHolder[0] = MqttReasonCode.Success;
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

    private void ResolveTopicAlias(
        MqttZeroClientConnection connection,
        MqttPublishPacket packet)
    {
        if (!packet.TopicAlias.HasValue)
        {
            if (packet.TopicName.IsEmpty)
            {
                throw new InvalidOperationException("The PUBLISH topic is empty.");
            }

            return;
        }

        ushort alias = packet.TopicAlias.Value;
        if (alias == 0 || alias > _options.TopicAliasMaximum)
        {
            throw new InvalidOperationException("The PUBLISH topic alias is invalid.");
        }

        if (!packet.TopicName.IsEmpty)
        {
            connection.SetInboundTopicAlias(alias, packet.TopicName);
            return;
        }

        if (!connection.TryGetInboundTopicAlias(alias, out ReadOnlyMemory<byte> topic))
        {
            throw new InvalidOperationException("The PUBLISH topic alias is unknown.");
        }

        packet.TopicName = new ReadOnlySequence<byte>(topic);
    }

}
