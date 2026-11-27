namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 DISCONNECT 报文。
/// </summary>
internal sealed class MqttDisconnectPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Disconnect;
        }
    }

    /// <summary>
    /// 获取或设置断开连接的原因码。
    /// </summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>
    /// 获取或设置客户端提供的会话过期覆盖值。
    /// </summary>
    public uint? SessionExpiryInterval { get; set; }

    /// <summary>获取或设置可选的原因字符串。</summary>
    public string? ReasonString { get; set; }

    /// <summary>获取或设置可选的服务端引用。</summary>
    public string? ServerReference { get; set; }

    /// <summary>获取或设置断开报文携带的有序用户属性。</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();
}
