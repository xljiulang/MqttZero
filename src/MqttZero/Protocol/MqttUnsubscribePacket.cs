using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 UNSUBSCRIBE 退订报文。
/// </summary>
internal sealed class MqttUnsubscribePacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Unsubscribe;
        }
    }

    /// <summary>
    /// 获取或设置报文标识符。
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>获取或设置退订报文携带的有序用户属性。</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();

    /// <summary>
    /// 获取或设置要移除的主题过滤器。
    /// </summary>
    public List<ReadOnlySequence<byte>> TopicFilters { get; set; } = [];
}
