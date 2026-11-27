namespace MqttZero.Protocol;

/// <summary>
/// MQTT 报文的抽象基类。
/// </summary>
internal abstract class MqttPacket
{
    /// <summary>
    /// 获取 MQTT 控制报文类型。
    /// </summary>
    public abstract MqttPacketType PacketType { get; }
}
