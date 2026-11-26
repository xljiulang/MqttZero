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
                    connection,
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
                    connection,
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
        MqttZeroClientConnection connection,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        MqttZeroQueuedMessage? acknowledgedMessage = null;
        bool protocolViolation = false;
        lock (session.SyncRoot)
        {
            if (session.InflightMessages.TryGetValue(
                    packetIdentifier,
                    out acknowledgedMessage))
            {
                if (acknowledgedMessage.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce)
                {
                    session.InflightMessages.Remove(packetIdentifier);
                }
                else
                {
                    acknowledgedMessage = null;
                    protocolViolation = true;
                }
            }
        }

        if (protocolViolation)
        {
            await connection.CloseAsync(
                MqttReasonCode.ProtocolError,
                cancellationToken).ConfigureAwait(false);
            return;
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
            MqttZeroSubscriptionDecision decision =
                await EvaluateSubscriptionAsync(
                    connection,
                    request,
                    packet.SubscriptionIdentifier,
                    candidate,
                    cancellationToken).ConfigureAwait(false);
            if (decision.CloseConnection)
            {
                await connection.CloseAsync(decision.ReasonCode, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            reasonCodes[index] = (byte)decision.ReasonCode;
            existed[index] = decision.Existed;
            changed |= decision.Outcome == MqttZeroSubscriptionOutcome.Subscribed;
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

        for (int index = 0; index < packet.Subscriptions.Count; index++)
        {
            MqttSubscriptionRequest request = packet.Subscriptions[index];
            if (!ShouldDeliverRetained(request, existed[index]))
            {
                continue;
            }

            await DeliverRetainedForSubscriptionAsync(
                session,
                request,
                packet.SubscriptionIdentifier,
                cancellationToken).ConfigureAwait(false);
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
            MqttZeroUnsubscriptionDecision decision =
                await EvaluateUnsubscriptionAsync(
                    connection,
                    packet.TopicFilters[index],
                    candidate,
                    cancellationToken).ConfigureAwait(false);
            if (decision.CloseConnection)
            {
                await connection.CloseAsync(decision.ReasonCode, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            reasonCodes[index] = (byte)decision.ReasonCode;
            changed |= decision.Changed;
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
