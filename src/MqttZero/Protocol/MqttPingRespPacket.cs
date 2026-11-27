namespace MqttZero.Protocol;

/// <summary>
/// MQTT PINGRESP 心跳响应报文。
/// </summary>
internal sealed class MqttPingRespPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.PingResp;
        }
    }
}
