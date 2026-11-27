namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 SUBSCRIBE 订阅报文。
/// </summary>
public sealed class MqttSubscribePacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Subscribe;
        }
    }

    /// <summary>
    /// Gets or sets the packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the subscription identifier.
    /// </summary>
    public uint? SubscriptionIdentifier { get; set; }

    /// <summary>Gets or sets the ordered subscribe user properties.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();

    /// <summary>
    /// Gets or sets the requested subscriptions.
    /// </summary>
    public List<MqttSubscriptionRequest> Subscriptions { get; set; } = [];
}
