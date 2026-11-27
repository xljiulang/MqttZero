namespace MqttZero.Protocol;

/// <summary>
/// QoS 1 的 PUBACK 确认报文。
/// </summary>
public sealed class MqttPubAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.PubAck;
        }
    }

    /// <summary>
    /// Gets or sets the acknowledged packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the acknowledgement reason code.
    /// </summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>Gets or sets the optional reason string.</summary>
    public string? ReasonString { get; set; }

    /// <summary>Gets or sets the ordered acknowledgement user properties.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } =
        Array.Empty<MqttUserProperty>();
}
