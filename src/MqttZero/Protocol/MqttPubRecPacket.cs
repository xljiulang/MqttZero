namespace MqttZero.Protocol;

/// <summary>
/// QoS 2 的 PUBREC 确认报文。
/// </summary>
internal sealed class MqttPubRecPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType => MqttPacketType.PubRec;

    /// <summary>获取或设置报文标识符。</summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>获取或设置原因码。</summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>获取或设置可选的原因字符串。</summary>
    public string? ReasonString { get; set; }

    /// <summary>获取或设置确认报文携带的有序用户属性。</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();
}
