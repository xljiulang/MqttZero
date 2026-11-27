namespace MqttZero.Protocol;

/// <summary>
/// 确认类报文使用的原因码。
/// </summary>
public enum MqttReasonCode : byte
{
    /// <summary>The operation succeeded.</summary>
    Success = 0x00,

    /// <summary>The client disconnected normally while publishing a Will.</summary>
    DisconnectWithWill = 0x04,

    /// <summary>No subscriptions matched the published message.</summary>
    NoMatchingSubscribers = 0x10,

    /// <summary>The server failed unexpectedly.</summary>
    UnspecifiedError = 0x80,

    /// <summary>The server encountered an implementation error.</summary>
    ImplementationSpecificError = 0x83,

    /// <summary>The server is unavailable.</summary>
    ServerUnavailable = 0x88,

    /// <summary>The client is banned.</summary>
    Banned = 0x8A,

    /// <summary>The client should use another server.</summary>
    UseAnotherServer = 0x9C,

    /// <summary>The server has moved.</summary>
    ServerMoved = 0x9D,

    /// <summary>The connection rate limit was exceeded.</summary>
    ConnectionRateExceeded = 0x9F,

    /// <summary>The user name or password is invalid.</summary>
    BadUserNameOrPassword = 0x86,

    /// <summary>Additional authentication is required.</summary>
    ContinueAuthentication = 0x18,

    /// <summary>The client requests re-authentication.</summary>
    Reauthenticate = 0x19,

    /// <summary>The packet is malformed.</summary>
    MalformedPacket = 0x81,

    /// <summary>The client violated protocol rules.</summary>
    ProtocolError = 0x82,

    /// <summary>The requested protocol version is unsupported.</summary>
    UnsupportedProtocolVersion = 0x84,

    /// <summary>The client identifier is invalid.</summary>
    ClientIdentifierNotValid = 0x85,

    /// <summary>The server is busy.</summary>
    ServerBusy = 0x89,

    /// <summary>The authentication method is unsupported.</summary>
    BadAuthenticationMethod = 0x8C,

    /// <summary>Subscription granted at QoS 1.</summary>
    GrantedQos1 = 0x01,

    /// <summary>Subscription granted at QoS 2.</summary>
    GrantedQos2 = 0x02,

    /// <summary>The requested QoS is not supported.</summary>
    QosNotSupported = 0x9B,

    /// <summary>Quota exceeded.</summary>
    QuotaExceeded = 0x97,

    /// <summary>The operation is not authorized.</summary>
    NotAuthorized = 0x87,

    /// <summary>No matching subscription existed.</summary>
    NoSubscriptionExisted = 0x11,

    /// <summary>The topic filter is invalid.</summary>
    TopicFilterInvalid = 0x8F,

    /// <summary>The topic name is invalid.</summary>
    TopicNameInvalid = 0x90,

    /// <summary>The packet identifier is already in use.</summary>
    PacketIdentifierInUse = 0x91,

    /// <summary>The packet identifier was not found.</summary>
    PacketIdentifierNotFound = 0x92,

    /// <summary>The receive maximum was exceeded.</summary>
    ReceiveMaximumExceeded = 0x93,

    /// <summary>The topic alias is invalid.</summary>
    TopicAliasInvalid = 0x94,

    /// <summary>The packet exceeds the negotiated maximum size.</summary>
    PacketTooLarge = 0x95,

    /// <summary>The message rate is too high.</summary>
    MessageRateTooHigh = 0x96,

    /// <summary>The server is shutting down.</summary>
    ServerShuttingDown = 0x8B,

    /// <summary>The client session was taken over by another connection.</summary>
    SessionTakenOver = 0x8E,

    /// <summary>The keep-alive timeout expired.</summary>
    KeepAliveTimeout = 0x8D,

    /// <summary>The payload format is invalid.</summary>
    PayloadFormatInvalid = 0x99,

    /// <summary>Retained messages are not supported.</summary>
    RetainNotSupported = 0x9A,

    /// <summary>Shared subscriptions are not supported.</summary>
    SharedSubscriptionsNotSupported = 0x9E,

    /// <summary>An administrative action caused the disconnect.</summary>
    AdministrativeAction = 0x98
}
