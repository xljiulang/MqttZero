using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Retained;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements packet dispatch, subscription, and unsubscription handling.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async ValueTask ProcessPacketAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttPacket packet,
        CancellationToken cancellationToken)
    {
        switch (packet)
        {
            case MqttSubscribePacket subscribePacket:
                await ProcessSubscribeAsync(session, connection, subscribePacket, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case MqttUnsubscribePacket unsubscribePacket:
                await ProcessUnsubscribeAsync(session, connection, unsubscribePacket, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case MqttPublishPacket publishPacket:
                ResolveTopicAlias(connection, publishPacket);
                await RoutePublishAsync(session, publishPacket, cancellationToken).ConfigureAwait(false);
                break;
            case MqttPingReqPacket:
                await connection.SendPingRespAsync(cancellationToken).ConfigureAwait(false);
                break;
            case MqttPubAckPacket pubAckPacket:
                await ProcessOutboundPubAckAsync(
                    session,
                    pubAckPacket.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPubRecPacket pubRecPacket:
                await ProcessOutboundPubRecAsync(
                    session,
                    connection,
                    pubRecPacket.PacketIdentifier,
                    pubRecPacket.ReasonCode,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPubRelPacket pubRelPacket:
                await ProcessInboundPubRelAsync(
                    session,
                    connection,
                    pubRelPacket.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPubCompPacket pubCompPacket:
                await ProcessOutboundPubCompAsync(
                    session,
                    pubCompPacket.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttDisconnectPacket disconnectPacket:
                if (disconnectPacket.ServerReference != null)
                {
                    await connection.CloseAsync(
                        MqttReasonCode.ProtocolError,
                        cancellationToken).ConfigureAwait(false);
                    break;
                }

                if (disconnectPacket.SessionExpiryInterval.HasValue)
                {
                    lock (session.SyncRoot)
                    {
                        session.ExpiryInterval = ClampSessionExpiry(
                            disconnectPacket.SessionExpiryInterval.Value);
                    }
                }

                break;
        }
    }

    private async ValueTask ProcessOutboundPubAckAsync(
        MqttZeroSessionState session,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        MqttZeroQueuedMessage? acknowledgedMessage = null;
        lock (session.SyncRoot)
        {
            if (session.InflightMessages.TryGetValue(
                    packetIdentifier,
                    out acknowledgedMessage) &&
                acknowledgedMessage.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce)
            {
                session.InflightMessages.Remove(packetIdentifier);
            }
        }

        if (acknowledgedMessage != null)
        {
            try
            {
                await DeleteMessageAsync(
                    session.ClientId,
                    acknowledgedMessage.MessageId,
                    cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                lock (session.SyncRoot)
                {
                    session.InflightMessages[packetIdentifier] = acknowledgedMessage;
                }

                throw;
            }

            acknowledgedMessage.Release();
        }

        SignalEgress(session);
    }

    private async ValueTask ProcessSubscribeAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttSubscribePacket packet,
        CancellationToken cancellationToken)
    {
        byte[] reasonCodes = new byte[packet.Subscriptions.Count];
        bool[] existed = new bool[packet.Subscriptions.Count];
        bool changed = false;
        List<MqttZeroSubscription> original;
        lock (session.SyncRoot)
        {
            original = [.. session.Subscriptions];
        }

        List<MqttZeroSubscription> candidate = new(original);
        for (int index = 0; index < packet.Subscriptions.Count; index++)
        {
            MqttSubscriptionRequest request = packet.Subscriptions[index];
            MqttZeroSubscribeContext context = new()
            {
                ConnectedClient = connection.ConnectedClient,
                TopicFilter = request.TopicFilter,
                QualityOfServiceLevel = request.QualityOfServiceLevel,
                NoLocal = request.NoLocal,
                RetainAsPublished = request.RetainAsPublished,
                RetainHandling = request.RetainHandling
            };
            MqttZeroInterceptionResult interception = MqttZeroInterceptionResult.Continue;
            foreach (IMqttZeroSubscribeInterceptor interceptor in _subscribeInterceptors)
            {
                interception = await interceptor.InterceptAsync(context, cancellationToken)
                    .ConfigureAwait(false);
                if (interception != MqttZeroInterceptionResult.Continue)
                {
                    break;
                }
            }

            if (interception == MqttZeroInterceptionResult.CloseConnection)
            {
                await connection.CloseAsync(context.ReasonCode, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (interception == MqttZeroInterceptionResult.Consume)
            {
                reasonCodes[index] = (byte)context.ReasonCode;
                continue;
            }

            byte[] topicFilter = request.TopicFilter.ToArray();
            int existingIndex = candidate.FindIndex(
                subscription => subscription.TopicFilter
                    .AsSpan()
                    .SequenceEqual(topicFilter));
            if (existingIndex < 0 &&
                candidate.Count >= _options.MaxSubscriptionsPerSession)
            {
                reasonCodes[index] = (byte)MqttReasonCode.QuotaExceeded;
                continue;
            }

            existed[index] = existingIndex >= 0;
            MqttZeroSubscription subscription = new(
                topicFilter,
                request.QualityOfServiceLevel,
                request.NoLocal,
                request.RetainAsPublished,
                packet.SubscriptionIdentifier);
            if (existingIndex >= 0)
            {
                candidate[existingIndex] = subscription;
            }
            else
            {
                candidate.Add(subscription);
            }

            reasonCodes[index] = request.QualityOfServiceLevel switch
            {
                MqttQualityOfServiceLevel.ExactlyOnce =>
                    (byte)MqttReasonCode.GrantedQos2,
                MqttQualityOfServiceLevel.AtLeastOnce =>
                    (byte)MqttReasonCode.GrantedQos1,
                _ => (byte)MqttReasonCode.Success
            };
            changed = true;
        }

        if (changed)
        {
            await CommitSubscriptionsAsync(
                session,
                original,
                candidate,
                cancellationToken).ConfigureAwait(false);
        }

        await connection.SendSubAckAsync(
            new MqttSubAckPacket
            {
                PacketIdentifier = packet.PacketIdentifier,
                ReasonCodes = reasonCodes
            },
            cancellationToken).ConfigureAwait(false);

        List<MqttZeroRetainedMessage> retainedMessages = [];
        for (int index = 0; index < packet.Subscriptions.Count; index++)
        {
            MqttSubscriptionRequest request = packet.Subscriptions[index];
            if (request.RetainHandling == MqttRetainHandling.DoNotSendOnSubscribe ||
                (request.RetainHandling ==
                    MqttRetainHandling.SendAtSubscribeIfNewSubscriptionOnly &&
                    existed[index]) ||
                request.TopicFilter.IsEmpty)
            {
                continue;
            }

            byte[] filter = request.TopicFilter.ToArray();
            if (filter.AsSpan().StartsWith("$share/"u8))
            {
                continue;
            }

            retainedMessages.Clear();
            MatchRetainedMessages(filter, retainedMessages);
            try
            {
                DateTimeOffset now = _timeProvider.GetUtcNow();
                for (int retainedIndex = 0;
                    retainedIndex < retainedMessages.Count;
                    retainedIndex++)
                {
                    MqttZeroRetainedMessage retainedMessage =
                        retainedMessages[retainedIndex];
                    if (retainedMessage.ExpiresAt.HasValue &&
                        retainedMessage.ExpiresAt.Value <= now)
                    {
                        continue;
                    }

                    MqttQualityOfServiceLevel qos =
                        (MqttQualityOfServiceLevel)Math.Min(
                            (byte)retainedMessage.QualityOfServiceLevel,
                            (byte)request.QualityOfServiceLevel);
                    ReadOnlySequence<byte> retainedProperties =
                        retainedMessage.Lease.Properties.Length == 0
                            ? ReadOnlySequence<byte>.Empty
                            : new ReadOnlySequence<byte>(
                                retainedMessage.Lease.Properties);
                    if (!MqttPublishPropertiesCodec.TryDecode(
                            retainedProperties,
                            out MqttPublishProperties properties,
                            out _,
                            out _))
                    {
                        throw new InvalidOperationException(
                            "The retained MQTT PUBLISH properties are invalid.");
                    }

                    properties = new MqttPublishProperties
                    {
                        PayloadFormatIndicator =
                            properties.PayloadFormatIndicator,
                        MessageExpiryInterval =
                            GetRemainingMessageExpiry(
                                retainedMessage.ExpiresAt,
                                now),
                        ContentType = properties.ContentType,
                        ResponseTopic = properties.ResponseTopic,
                        CorrelationData = properties.CorrelationData,
                        UserProperties = properties.UserProperties
                    };
                    MqttPublishPacket retainedPacket = new()
                    {
                        TopicName = new ReadOnlySequence<byte>(
                            retainedMessage.Topic),
                        Payload = new ReadOnlySequence<byte>(
                            retainedMessage.Payload),
                        Properties = properties,
                        QualityOfServiceLevel = qos,
                        Retain = true,
                        SubscriptionIdentifier =
                            packet.SubscriptionIdentifier
                    };
                    await DeliverAsync(
                        session,
                        retainedPacket,
                        qos,
                        true,
                        packet.SubscriptionIdentifier,
                        cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                ReleaseRetainedMessages(retainedMessages);
            }
        }
    }

    private async ValueTask ProcessUnsubscribeAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttUnsubscribePacket packet,
        CancellationToken cancellationToken)
    {
        byte[] reasonCodes = new byte[packet.TopicFilters.Count];
        bool changed = false;
        List<MqttZeroSubscription> original;
        lock (session.SyncRoot)
        {
            original = [.. session.Subscriptions];
        }

        List<MqttZeroSubscription> candidate = new(original);
        for (int index = 0; index < packet.TopicFilters.Count; index++)
        {
            byte[] topicFilter = packet.TopicFilters[index].ToArray();
            MqttZeroUnsubscribeContext context = new()
            {
                ConnectedClient = connection.ConnectedClient,
                TopicFilter = new ReadOnlySequence<byte>(topicFilter)
            };
            MqttZeroInterceptionResult interception = MqttZeroInterceptionResult.Continue;
            foreach (IMqttZeroUnsubscribeInterceptor interceptor in _unsubscribeInterceptors)
            {
                interception = await interceptor.InterceptAsync(context, cancellationToken)
                    .ConfigureAwait(false);
                if (interception != MqttZeroInterceptionResult.Continue)
                {
                    break;
                }
            }

            if (interception == MqttZeroInterceptionResult.CloseConnection)
            {
                await connection.CloseAsync(context.ReasonCode, cancellationToken).ConfigureAwait(false);
                return;
            }

            if (interception == MqttZeroInterceptionResult.Consume)
            {
                reasonCodes[index] = (byte)context.ReasonCode;
                continue;
            }

            int existingIndex = candidate.FindIndex(
                subscription => subscription.TopicFilter
                    .AsSpan()
                    .SequenceEqual(topicFilter));
            bool removed = existingIndex >= 0;
            if (removed)
            {
                candidate.RemoveAt(existingIndex);
                changed = true;
            }

            reasonCodes[index] = removed
                ? (byte)MqttReasonCode.Success
                : (byte)MqttReasonCode.NoSubscriptionExisted;
        }

        if (changed)
        {
            await CommitSubscriptionsAsync(
                session,
                original,
                candidate,
                cancellationToken).ConfigureAwait(false);
        }

        await connection.SendUnsubAckAsync(
            new MqttUnsubAckPacket
            {
                PacketIdentifier = packet.PacketIdentifier,
                ReasonCodes = reasonCodes
            },
            cancellationToken).ConfigureAwait(false);
    }

}
