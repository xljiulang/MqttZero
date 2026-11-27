namespace MqttZero.Protocol;

/// <summary>
/// MQTT 控制报文类型。
/// </summary>
internal enum MqttPacketType : byte
{
    /// <summary>Client connection request.</summary>
    Connect = 1,

    /// <summary>Connection acknowledgement.</summary>
    ConnAck = 2,

    /// <summary>Application message publication.</summary>
    Publish = 3,

    /// <summary>QoS 1 publication acknowledgement.</summary>
    PubAck = 4,

    /// <summary>QoS 2 publication received.</summary>
    PubRec = 5,

    /// <summary>QoS 2 publication release.</summary>
    PubRel = 6,

    /// <summary>QoS 2 publication complete.</summary>
    PubComp = 7,

    /// <summary>Topic subscription request.</summary>
    Subscribe = 8,

    /// <summary>Topic subscription acknowledgement.</summary>
    SubAck = 9,

    /// <summary>Topic unsubscription request.</summary>
    Unsubscribe = 10,

    /// <summary>Topic unsubscription acknowledgement.</summary>
    UnsubAck = 11,

    /// <summary>Keep-alive request.</summary>
    PingReq = 12,

    /// <summary>Keep-alive response.</summary>
    PingResp = 13,

    /// <summary>Disconnect notification.</summary>
    Disconnect = 14
}
