namespace MqttZero.Protocol;

/// <summary>
/// QoS 2 的 PUBREC 确认报文。
/// </summary>
internal sealed class MqttPubRecPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType => MqttPacketType.PubRec;

    /// <summary>Gets or sets the packet identifier.</summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>Gets or sets the reason code.</summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>Gets or sets the optional reason string.</summary>
    public string? ReasonString { get; set; }

    /// <summary>Gets or sets the ordered acknowledgement user properties.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();
}
