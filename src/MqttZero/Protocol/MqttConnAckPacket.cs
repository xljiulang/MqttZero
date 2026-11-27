using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 CONNACK 报文。
/// </summary>
internal sealed class MqttConnAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.ConnAck;
        }
    }

    /// <summary>
    /// 获取或设置服务端是否恢复了已有会话。
    /// </summary>
    public bool SessionPresent { get; set; }

    /// <summary>
    /// 获取或设置连接报文的原因码。
    /// </summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>
    /// 获取或设置服务端接受的入站主题别名上限。
    /// </summary>
    public ushort? TopicAliasMaximum { get; set; }

    /// <summary>
    /// 获取或设置服务端的接收上限。
    /// </summary>
    public ushort? ReceiveMaximum { get; set; }

    /// <summary>
    /// 获取或设置服务端接受的最大报文大小。
    /// </summary>
    public uint? MaximumPacketSize { get; set; }

    /// <summary>
    /// 获取或设置可选的原因字符串。
    /// </summary>
    public string? ReasonString { get; set; }

    /// <summary>
    /// 获取或设置服务端接受的会话过期时间。
    /// </summary>
    public uint? SessionExpiryInterval { get; set; }

    /// <summary>
    /// 获取或设置服务端分配的客户端标识。
    /// </summary>
    public string? AssignedClientIdentifier { get; set; }

    /// <summary>
    /// 获取或设置服务端支持的最大服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel? MaximumQos { get; set; }

    /// <summary>
    /// 获取或设置服务端是否支持保留消息。
    /// </summary>
    public bool? RetainAvailable { get; set; }

    /// <summary>
    /// 获取或设置服务端是否支持通配符订阅。
    /// </summary>
    public bool? WildcardSubscriptionAvailable { get; set; }

    /// <summary>
    /// 获取或设置服务端是否支持订阅标识符。
    /// </summary>
    public bool? SubscriptionIdentifierAvailable { get; set; }

    /// <summary>
    /// 获取或设置服务端是否支持共享订阅。
    /// </summary>
    public bool? SharedSubscriptionAvailable { get; set; }

    /// <summary>
    /// 获取或设置服务端覆盖的保活间隔（秒）。
    /// </summary>
    public ushort? ServerKeepAlive { get; set; }

    /// <summary>
    /// 获取或设置响应信息。
    /// </summary>
    public string? ResponseInformation { get; set; }

    /// <summary>
    /// 获取或设置服务端引用。
    /// </summary>
    public string? ServerReference { get; set; }

    /// <summary>
    /// 获取或设置增强认证方法。
    /// </summary>
    public string? AuthenticationMethod { get; set; }

    /// <summary>
    /// 获取或设置增强认证数据。
    /// </summary>
    public ReadOnlyMemory<byte>? AuthenticationData { get; set; }

    /// <summary>
    /// 获取或设置 CONNACK 携带的有序用户属性。
    /// </summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();
}
