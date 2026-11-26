using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Retained;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Text;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private static ReadOnlySpan<byte> SharedSubscriptionPrefix
    {
        get
        {
            return "$share/"u8;
        }
    }

    private async ValueTask<MqttZeroSubscriptionDecision> EvaluateSubscriptionAsync(
        MqttZeroClientConnection connection,
        MqttSubscriptionRequest request,
        uint? subscriptionIdentifier,
        List<MqttZeroSubscription> candidate,
        CancellationToken cancellationToken)
    {
        MqttZeroSubscribeContext context = new()
        {
            ConnectedClient = connection.ConnectedClient,
            TopicFilter = request.TopicFilter,
            QualityOfServiceLevel = request.QualityOfServiceLevel,
            NoLocal = request.NoLocal,
            RetainAsPublished = request.RetainAsPublished,
            RetainHandling = request.RetainHandling
        };
        MqttZeroInterceptionResult interception =
            await RunSubscribeInterceptorsAsync(context, cancellationToken)
                .ConfigureAwait(false);
        if (interception == MqttZeroInterceptionResult.CloseConnection)
        {
            return MqttZeroSubscriptionDecision.Closed(context.ReasonCode);
        }

        if (interception == MqttZeroInterceptionResult.Consume)
        {
            return MqttZeroSubscriptionDecision.Consumed(context.ReasonCode);
        }

        byte[] filter = request.TopicFilter.ToArray();
        int existingIndex = IndexOfSubscription(candidate, filter);
        bool existed = existingIndex >= 0;
        if (!existed && candidate.Count >= _options.MaxSubscriptionsPerSession)
        {
            return MqttZeroSubscriptionDecision.QuotaExceeded();
        }

        MqttZeroSubscription subscription = new(
            filter,
            request.QualityOfServiceLevel,
            request.NoLocal,
            request.RetainAsPublished,
            subscriptionIdentifier);
        if (existed)
        {
            candidate[existingIndex] = subscription;
        }
        else
        {
            candidate.Add(subscription);
        }

        return MqttZeroSubscriptionDecision.Subscribed(
            existed,
            GetGrantedReasonCode(request.QualityOfServiceLevel));
    }

    private async ValueTask<MqttZeroUnsubscriptionDecision> EvaluateUnsubscriptionAsync(
        MqttZeroClientConnection connection,
        ReadOnlySequence<byte> encodedFilter,
        List<MqttZeroSubscription> candidate,
        CancellationToken cancellationToken)
    {
        MqttZeroUnsubscribeContext context = new()
        {
            ConnectedClient = connection.ConnectedClient,
            TopicFilter = encodedFilter
        };
        MqttZeroInterceptionResult interception =
            await RunUnsubscribeInterceptorsAsync(context, cancellationToken)
                .ConfigureAwait(false);
        if (interception == MqttZeroInterceptionResult.CloseConnection)
        {
            return MqttZeroUnsubscriptionDecision.Closed(context.ReasonCode);
        }

        if (interception == MqttZeroInterceptionResult.Consume)
        {
            return MqttZeroUnsubscriptionDecision.Consumed(context.ReasonCode);
        }

        byte[] filter = encodedFilter.ToArray();
        int existingIndex = IndexOfSubscription(candidate, filter);
        if (existingIndex < 0)
        {
            return MqttZeroUnsubscriptionDecision.Unsubscribed(changed: false);
        }

        candidate.RemoveAt(existingIndex);
        return MqttZeroUnsubscriptionDecision.Unsubscribed(changed: true);
    }

    /// <summary>
    /// Finds one subscription by topic filter. Packet-local scans are bounded by
    /// the subscription count, so a linear scan avoids a per-request string key.
    /// </summary>
    private static int IndexOfSubscription(
        List<MqttZeroSubscription> subscriptions,
        ReadOnlySpan<byte> filter)
    {
        for (int index = 0; index < subscriptions.Count; index++)
        {
            if (subscriptions[index].TopicFilter.AsSpan().SequenceEqual(filter))
            {
                return index;
            }
        }

        return -1;
    }

    private static MqttReasonCode GetGrantedReasonCode(
        MqttQualityOfServiceLevel qualityOfServiceLevel)
    {
        return qualityOfServiceLevel switch
        {
            MqttQualityOfServiceLevel.ExactlyOnce => MqttReasonCode.GrantedQos2,
            MqttQualityOfServiceLevel.AtLeastOnce => MqttReasonCode.GrantedQos1,
            _ => MqttReasonCode.Success
        };
    }

    private static string GetTopicFilterText(ReadOnlySequence<byte> topicFilter)
    {
        if (topicFilter.IsSingleSegment)
        {
            return Encoding.UTF8.GetString(topicFilter.FirstSpan);
        }

        return Encoding.UTF8.GetString(topicFilter.ToArray());
    }

    private static bool ShouldDeliverRetained(
        MqttSubscriptionRequest request,
        bool existed)
    {
        if (request.RetainHandling == MqttRetainHandling.DoNotSendOnSubscribe)
        {
            return false;
        }

        return request.RetainHandling !=
                MqttRetainHandling.SendAtSubscribeIfNewSubscriptionOnly ||
            !existed;
    }

    private async ValueTask DeliverRetainedForSubscriptionAsync(
        MqttZeroSessionState session,
        MqttSubscriptionRequest request,
        uint? subscriptionIdentifier,
        CancellationToken cancellationToken)
    {
        if (request.TopicFilter.IsEmpty)
        {
            return;
        }

        byte[] filter = request.TopicFilter.ToArray();
        if (filter.AsSpan().StartsWith(SharedSubscriptionPrefix))
        {
            return;
        }

        List<MqttZeroRetainedMessage> retainedMessages = [];
        MatchRetainedMessages(filter, retainedMessages);
        try
        {
            DateTimeOffset now = _timeProvider.GetUtcNow();
            for (int index = 0; index < retainedMessages.Count; index++)
            {
                MqttZeroRetainedMessage retained = retainedMessages[index];
                if (retained.ExpiresAt.HasValue &&
                    retained.ExpiresAt.Value <= now)
                {
                    continue;
                }

                MqttPublishPacket? packet = CreateRetainedPublish(
                    retained,
                    request,
                    subscriptionIdentifier,
                    now);
                if (packet == null)
                {
                    continue;
                }

                await DeliverAsync(
                    session,
                    packet,
                    packet.QualityOfServiceLevel,
                    true,
                    subscriptionIdentifier,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            ReleaseRetainedMessages(retainedMessages);
        }
    }

    private MqttPublishPacket? CreateRetainedPublish(
        MqttZeroRetainedMessage retained,
        MqttSubscriptionRequest request,
        uint? subscriptionIdentifier,
        DateTimeOffset now)
    {
        MqttPublishProperties? properties = CreateRetainedPublishProperties(
            retained,
            now);
        if (properties == null)
        {
            return null;
        }

        MqttQualityOfServiceLevel qos =
            (MqttQualityOfServiceLevel)Math.Min(
                (byte)retained.QualityOfServiceLevel,
                (byte)request.QualityOfServiceLevel);
        return new MqttPublishPacket
        {
            TopicName = new ReadOnlySequence<byte>(retained.Topic),
            Payload = new ReadOnlySequence<byte>(retained.Payload),
            Properties = properties,
            QualityOfServiceLevel = qos,
            Retain = true,
            SubscriptionIdentifier = subscriptionIdentifier
        };
    }

    private MqttPublishProperties? CreateRetainedPublishProperties(
        MqttZeroRetainedMessage retained,
        DateTimeOffset now)
    {
        ReadOnlySequence<byte> retainedProperties =
            retained.Lease.Properties.Length == 0
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(retained.Lease.Properties);
        if (!MqttPublishPropertiesCodec.TryDecode(
                retainedProperties,
                out MqttPublishProperties properties,
                out _,
                out _))
        {
            MqttZeroBrokerLogging.LogInvalidRetainedProperties(
                _logger,
                Encoding.UTF8.GetString(retained.Topic.Span));
            return null;
        }

        return new MqttPublishProperties
        {
            PayloadFormatIndicator = properties.PayloadFormatIndicator,
            MessageExpiryInterval = GetRemainingMessageExpiry(
                retained.ExpiresAt,
                now),
            ContentType = properties.ContentType,
            ResponseTopic = properties.ResponseTopic,
            CorrelationData = properties.CorrelationData,
            UserProperties = properties.UserProperties
        };
    }
}
