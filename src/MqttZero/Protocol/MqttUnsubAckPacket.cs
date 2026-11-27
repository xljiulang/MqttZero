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
    /// 获取或设置报文标识符。
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>获取或设置可选的原因字符串。</summary>
    public string? ReasonString { get; set; }

    /// <summary>获取或设置确认报文携带的有序用户属性。</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();

    /// <summary>
    /// 获取或设置每个退订过滤器对应的原因码。
    /// </summary>
    public ReadOnlyMemory<byte> ReasonCodes { get; set; }
}
