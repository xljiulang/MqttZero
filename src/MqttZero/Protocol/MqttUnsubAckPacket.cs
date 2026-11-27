using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 UNSUBACK 退订确认报文。
/// </summary>
internal sealed class MqttUnsubAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.UnsubAck;
        }
    }

    /// <summary>
    /// Gets or sets the packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>Gets or sets the optional reason string.</summary>
    public string? ReasonString { get; set; }

    /// <summary>Gets or sets the ordered acknowledgement user properties.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();

    /// <summary>
    /// Gets or sets one reason code per unsubscribed filter.
    /// </summary>
    public ReadOnlyMemory<byte> ReasonCodes { get; set; }
}
