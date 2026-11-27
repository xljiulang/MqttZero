using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;

namespace MqttZero.Server.Broker;

/// <summary>
/// MQTT 5 Broker 主体，按连接、会话、路由、投递与生命周期拆成多个分部文件。
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// 运行发布Interceptors。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
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

    /// <summary>
    /// 运行订阅Interceptors。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
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

    /// <summary>
    /// 运行退订Interceptors。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
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

    /// <summary>
    /// Assign发布上下文。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="context">上下文。</param>
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

    /// <summary>
    /// 拒绝发布。
    /// </summary>
    /// <param name="senderConnection">sender连接。</param>
    /// <param name="lease">租约。</param>
    /// <param name="interception">对应的 interception。</param>
    /// <param name="context">上下文。</param>
    /// <param name="suppressPublisherAck">suppressPublisher确认。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
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
