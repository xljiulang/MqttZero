namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 SUBSCRIBE 订阅报文。
/// </summary>
internal sealed class MqttSubscribePacket : MqttPacket
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
    /// 获取或设置报文标识符。
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// 获取或设置订阅标识符。
    /// </summary>
    public uint? SubscriptionIdentifier { get; set; }

    /// <summary>获取或设置订阅报文携带的有序用户属性。</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();

    /// <summary>
    /// 获取或设置请求的订阅列表。
    /// </summary>
    public List<MqttSubscriptionRequest> Subscriptions { get; set; } = [];
}
