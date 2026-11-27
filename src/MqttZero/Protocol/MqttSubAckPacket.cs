using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 SUBACK 订阅确认报文。
/// </summary>
internal sealed class MqttSubAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.SubAck;
        }
    }

    /// <summary>
    /// Gets or sets the acknowledged packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>Gets or sets the optional reason string.</summary>
    public string? ReasonString { get; set; }

    /// <summary>Gets or sets the ordered acknowledgement user properties.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();

    /// <summary>
    /// Gets or sets the reason code for each subscription request.
    /// </summary>
    public ReadOnlyMemory<byte> ReasonCodes { get; set; }
}
