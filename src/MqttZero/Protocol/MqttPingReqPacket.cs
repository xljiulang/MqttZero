namespace MqttZero.Protocol;

/// <summary>
/// MQTT PINGREQ 心跳请求报文。
/// </summary>
public sealed class MqttPingReqPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.PingReq;
        }
    }
}
