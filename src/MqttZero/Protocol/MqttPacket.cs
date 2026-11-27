namespace MqttZero.Protocol;

/// <summary>
/// MQTT 报文的抽象基类。
/// </summary>
public abstract class MqttPacket
{
    /// <summary>
    /// Gets the MQTT control packet type.
    /// </summary>
    public abstract MqttPacketType PacketType { get; }
}
