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
    /// 获取或设置被确认的报文标识符。
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>获取或设置可选的原因字符串。</summary>
    public string? ReasonString { get; set; }

    /// <summary>获取或设置确认报文携带的有序用户属性。</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();

    /// <summary>
    /// 获取或设置每个订阅请求对应的原因码。
    /// </summary>
    public ReadOnlyMemory<byte> ReasonCodes { get; set; }
}
