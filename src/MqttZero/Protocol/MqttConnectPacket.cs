using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 CONNECT 报文。
/// </summary>
internal sealed class MqttConnectPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Connect;
        }
    }

    /// <summary>
    /// 获取或设置协商后的协议版本。
    /// </summary>
    public MqttProtocolVersion ProtocolVersion { get; set; } = MqttProtocolVersion.V500;

    /// <summary>
    /// 获取或设置客户端是否请求清理会话。
    /// </summary>
    public bool CleanStart { get; set; }

    /// <summary>
    /// 获取或设置保活间隔（秒）。
    /// </summary>
    public ushort KeepAliveSeconds { get; set; }

    /// <summary>
    /// 获取或设置 UTF-8 客户端标识。
    /// </summary>
    public ReadOnlySequence<byte> ClientId { get; set; }

    /// <summary>
    /// 获取或设置客户端是否请求响应信息。
    /// </summary>
    public bool RequestResponseInformation { get; set; }

    /// <summary>
    /// 获取或设置服务端是否可以发送问题诊断信息。
    /// </summary>
    public bool RequestProblemInformation { get; set; } = true;

    /// <summary>
    /// 获取或设置请求的会话过期时间（秒）。
    /// </summary>
    public uint? SessionExpiryInterval { get; set; }

    /// <summary>
    /// 获取或设置客户端请求的接收上限。
    /// </summary>
    public ushort ReceiveMaximum { get; set; } = ushort.MaxValue;

    /// <summary>
    /// 获取或设置客户端接受的最大报文大小。
    /// </summary>
    public uint MaximumPacketSize { get; set; } = uint.MaxValue;

    /// <summary>
    /// 获取或设置客户端接受的入站主题别名上限。
    /// </summary>
    public ushort TopicAliasMaximum { get; set; }

    /// <summary>
    /// 获取或设置 CONNECT 携带的有序用户属性。
    /// </summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } = Array.Empty<MqttUserProperty>();

    /// <summary>
    /// 获取或设置增强认证方法。
    /// </summary>
    public string? AuthenticationMethod { get; set; }

    /// <summary>
    /// 获取或设置增强认证数据。
    /// </summary>
    public ReadOnlyMemory<byte>? AuthenticationData { get; set; }

    /// <summary>
    /// 获取或设置是否携带用户名。
    /// </summary>
    public bool HasUserName { get; set; }

    /// <summary>
    /// 获取或设置 UTF-8 用户名。
    /// </summary>
    public ReadOnlySequence<byte> UserName { get; set; }

    /// <summary>
    /// 获取或设置是否携带密码。
    /// </summary>
    public bool HasPassword { get; set; }

    /// <summary>
    /// 获取或设置密码字节。
    /// </summary>
    public ReadOnlySequence<byte> Password { get; set; }

    /// <summary>
    /// 获取或设置是否携带遗嘱消息。
    /// </summary>
    public bool HasWill { get; set; }

    /// <summary>
    /// 获取或设置遗嘱的服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel WillQualityOfServiceLevel { get; set; }

    /// <summary>
    /// 获取或设置遗嘱消息是否作为保留消息发布。
    /// </summary>
    public bool WillRetain { get; set; }

    /// <summary>
    /// 获取或设置随遗嘱传递的 MQTT 5 属性。
    /// </summary>
    public MqttPublishProperties WillProperties { get; set; } = MqttPublishProperties.Empty;

    /// <summary>
    /// 获取或设置遗嘱延迟发布的秒数。
    /// </summary>
    public uint WillDelayInterval { get; set; }

    /// <summary>
    /// 获取或设置 UTF-8 遗嘱主题。
    /// </summary>
    public ReadOnlySequence<byte> WillTopic { get; set; }

    /// <summary>
    /// 获取或设置遗嘱负载。
    /// </summary>
    public ReadOnlySequence<byte> WillPayload { get; set; }

    /// <summary>
    /// WillMessageExpiryInterval 的值。
    /// </summary>
    internal uint WillMessageExpiryInterval
    {
        get
        {
            return WillProperties.MessageExpiryInterval ?? 0;
        }

        set
        {
            WillProperties = new MqttPublishProperties
            {
                PayloadFormatIndicator = WillProperties.PayloadFormatIndicator,
                MessageExpiryInterval = value == 0 ? null : value,
                ContentType = WillProperties.ContentType,
                ResponseTopic = WillProperties.ResponseTopic,
                CorrelationData = WillProperties.CorrelationData,
                UserProperties = WillProperties.UserProperties
            };
        }
    }
}
