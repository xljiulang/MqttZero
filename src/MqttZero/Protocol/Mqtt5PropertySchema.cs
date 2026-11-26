namespace MqttZero.Protocol;

internal static class Mqtt5PropertySchema
{
    private static readonly ulong ConnectProperties =
        PropertyMask(
            Mqtt5PropertyId.SessionExpiryInterval,
            Mqtt5PropertyId.AuthenticationMethod,
            Mqtt5PropertyId.AuthenticationData,
            Mqtt5PropertyId.RequestProblemInformation,
            Mqtt5PropertyId.RequestResponseInformation,
            Mqtt5PropertyId.ReceiveMaximum,
            Mqtt5PropertyId.TopicAliasMaximum,
            Mqtt5PropertyId.UserProperty,
            Mqtt5PropertyId.MaximumPacketSize);
    private static readonly ulong WillProperties =
        PropertyMask(
            Mqtt5PropertyId.PayloadFormatIndicator,
            Mqtt5PropertyId.MessageExpiryInterval,
            Mqtt5PropertyId.ContentType,
            Mqtt5PropertyId.ResponseTopic,
            Mqtt5PropertyId.CorrelationData,
            Mqtt5PropertyId.WillDelayInterval,
            Mqtt5PropertyId.UserProperty);
    private static readonly ulong ConnAckProperties =
        PropertyMask(
            Mqtt5PropertyId.SessionExpiryInterval,
            Mqtt5PropertyId.AssignedClientIdentifier,
            Mqtt5PropertyId.ServerKeepAlive,
            Mqtt5PropertyId.AuthenticationMethod,
            Mqtt5PropertyId.AuthenticationData,
            Mqtt5PropertyId.ResponseInformation,
            Mqtt5PropertyId.ServerReference,
            Mqtt5PropertyId.ReasonString,
            Mqtt5PropertyId.ReceiveMaximum,
            Mqtt5PropertyId.TopicAliasMaximum,
            Mqtt5PropertyId.MaximumQos,
            Mqtt5PropertyId.RetainAvailable,
            Mqtt5PropertyId.UserProperty,
            Mqtt5PropertyId.MaximumPacketSize,
            Mqtt5PropertyId.WildcardSubscriptionAvailable,
            Mqtt5PropertyId.SubscriptionIdentifierAvailable,
            Mqtt5PropertyId.SharedSubscriptionAvailable);
    private static readonly ulong PublishProperties =
        PropertyMask(
            Mqtt5PropertyId.PayloadFormatIndicator,
            Mqtt5PropertyId.MessageExpiryInterval,
            Mqtt5PropertyId.ContentType,
            Mqtt5PropertyId.ResponseTopic,
            Mqtt5PropertyId.CorrelationData,
            Mqtt5PropertyId.SubscriptionIdentifier,
            Mqtt5PropertyId.TopicAlias,
            Mqtt5PropertyId.UserProperty);
    private static readonly ulong AcknowledgementProperties = PropertyMask(
        Mqtt5PropertyId.ReasonString,
        Mqtt5PropertyId.UserProperty);
    private static readonly ulong SubscribeProperties = PropertyMask(
        Mqtt5PropertyId.SubscriptionIdentifier,
        Mqtt5PropertyId.UserProperty);
    private static readonly ulong UnsubscribeProperties = PropertyMask(
        Mqtt5PropertyId.UserProperty);
    private static readonly ulong DisconnectProperties =
        PropertyMask(
            Mqtt5PropertyId.SessionExpiryInterval,
            Mqtt5PropertyId.ServerReference,
            Mqtt5PropertyId.ReasonString,
            Mqtt5PropertyId.UserProperty);

    public static bool IsAllowed(MqttPacketType packetType, byte identifier)
    {
        ulong properties = packetType switch
        {
            MqttPacketType.Connect => ConnectProperties,
            MqttPacketType.ConnAck => ConnAckProperties,
            MqttPacketType.Publish => PublishProperties,
            MqttPacketType.PubAck => AcknowledgementProperties,
            MqttPacketType.PubRec => AcknowledgementProperties,
            MqttPacketType.PubRel => AcknowledgementProperties,
            MqttPacketType.PubComp => AcknowledgementProperties,
            MqttPacketType.Subscribe => SubscribeProperties,
            MqttPacketType.SubAck => AcknowledgementProperties,
            MqttPacketType.Unsubscribe => UnsubscribeProperties,
            MqttPacketType.UnsubAck => AcknowledgementProperties,
            MqttPacketType.Disconnect => DisconnectProperties,
            _ => 0
        };
        return identifier < 64 && (properties & (1UL << identifier)) != 0;
    }

    public static bool IsAllowedWillProperty(byte identifier)
    {
        return identifier < 64 && (WillProperties & (1UL << identifier)) != 0;
    }

    private static ulong PropertyMask(params ReadOnlySpan<byte> identifiers)
    {
        ulong mask = 0;
        foreach (byte identifier in identifiers)
        {
            mask |= 1UL << identifier;
        }

        return mask;
    }
}
