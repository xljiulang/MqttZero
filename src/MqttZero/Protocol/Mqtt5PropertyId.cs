namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 属性标识符常量。
/// </summary>
internal static class Mqtt5PropertyId
{
    public const byte PayloadFormatIndicator = 0x01;
    public const byte MessageExpiryInterval = 0x02;
    public const byte ContentType = 0x03;
    public const byte ResponseTopic = 0x08;
    public const byte CorrelationData = 0x09;
    public const byte SubscriptionIdentifier = 0x0B;
    public const byte SessionExpiryInterval = 0x11;
    public const byte AssignedClientIdentifier = 0x12;
    public const byte ServerKeepAlive = 0x13;
    public const byte AuthenticationMethod = 0x15;
    public const byte AuthenticationData = 0x16;
    public const byte RequestProblemInformation = 0x17;
    public const byte WillDelayInterval = 0x18;
    public const byte RequestResponseInformation = 0x19;
    public const byte ResponseInformation = 0x1A;
    public const byte ServerReference = 0x1C;
    public const byte ReasonString = 0x1F;
    public const byte ReceiveMaximum = 0x21;
    public const byte TopicAliasMaximum = 0x22;
    public const byte TopicAlias = 0x23;
    public const byte MaximumQos = 0x24;
    public const byte RetainAvailable = 0x25;
    public const byte UserProperty = 0x26;
    public const byte MaximumPacketSize = 0x27;
    public const byte WildcardSubscriptionAvailable = 0x28;
    public const byte SubscriptionIdentifierAvailable = 0x29;
    public const byte SharedSubscriptionAvailable = 0x2A;
}
