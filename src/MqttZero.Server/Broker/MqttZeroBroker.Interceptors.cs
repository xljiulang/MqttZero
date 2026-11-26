using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private async ValueTask<MqttZeroInterceptionResult> RunPublishInterceptorsAsync(
        MqttZeroPublishContext context,
        CancellationToken cancellationToken)
    {
        for (int index = 0; index < _publishInterceptors.Length; index++)
        {
            MqttZeroInterceptionResult result = await _publishInterceptors[index]
                .InterceptAsync(context, cancellationToken)
                .ConfigureAwait(false);
            if (result != MqttZeroInterceptionResult.Continue)
            {
                return result;
            }
        }

        return MqttZeroInterceptionResult.Continue;
    }

    private async ValueTask<MqttZeroInterceptionResult> RunSubscribeInterceptorsAsync(
        MqttZeroSubscribeContext context,
        CancellationToken cancellationToken)
    {
        for (int index = 0; index < _subscribeInterceptors.Length; index++)
        {
            MqttZeroInterceptionResult result = await _subscribeInterceptors[index]
                .InterceptAsync(context, cancellationToken)
                .ConfigureAwait(false);
            if (result != MqttZeroInterceptionResult.Continue)
            {
                return result;
            }
        }

        return MqttZeroInterceptionResult.Continue;
    }

    private async ValueTask<MqttZeroInterceptionResult> RunUnsubscribeInterceptorsAsync(
        MqttZeroUnsubscribeContext context,
        CancellationToken cancellationToken)
    {
        for (int index = 0; index < _unsubscribeInterceptors.Length; index++)
        {
            MqttZeroInterceptionResult result = await _unsubscribeInterceptors[index]
                .InterceptAsync(context, cancellationToken)
                .ConfigureAwait(false);
            if (result != MqttZeroInterceptionResult.Continue)
            {
                return result;
            }
        }

        return MqttZeroInterceptionResult.Continue;
    }

    private static void AssignPublishContext(
        MqttPublishPacket packet,
        MqttZeroPublishContext context)
    {
        packet.TopicName = context.Topic;
        packet.Payload = context.Payload;
        packet.Properties = context.Properties;
        packet.QualityOfServiceLevel = context.QualityOfServiceLevel;
        packet.Retain = context.Retain;
    }

    private static async ValueTask<MqttReasonCode> RejectPublishAsync(
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        MqttZeroInterceptionResult interception,
        MqttZeroPublishContext context,
        bool suppressPublisherAck,
        CancellationToken cancellationToken)
    {
        if (interception == MqttZeroInterceptionResult.CloseConnection)
        {
            await senderConnection.CloseAsync(
                context.ResultReasonCode == MqttReasonCode.Success
                    ? MqttReasonCode.NotAuthorized
                    : context.ResultReasonCode,
                cancellationToken).ConfigureAwait(false);
        }
        else if (!suppressPublisherAck &&
            lease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
        {
            await senderConnection.SendPubAckAsync(
                lease.PacketIdentifier,
                context.ResultReasonCode,
                cancellationToken).ConfigureAwait(false);
        }

        return context.ResultReasonCode;
    }
}
