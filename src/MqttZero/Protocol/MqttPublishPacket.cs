using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 PUBLISH 发布报文。
/// </summary>
internal sealed class MqttPublishPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Publish;
        }
    }

    /// <summary>
    /// 获取或设置 UTF-8 主题名。
    /// </summary>
    public ReadOnlySequence<byte> TopicName { get; set; }

    /// <summary>
    /// 获取或设置随发布传递的 MQTT 5 属性。
    /// </summary>
    public MqttPublishProperties Properties { get; set; } = MqttPublishProperties.Empty;

    /// <summary>
    /// 获取或设置挂在该投递上的订阅标识符。
    /// </summary>
    public IReadOnlyList<uint> SubscriptionIdentifiers { get; set; } = Array.Empty<uint>();

    /// <summary>
    /// 获取或设置 MQTT 5 主题别名。
    /// </summary>
    public ushort? TopicAlias { get; set; }

    /// <summary>
    /// 获取或设置负载序列。
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; set; }

    /// <summary>
    /// 获取或设置投递的服务质量等级。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// 获取或设置 QoS 流程中的报文标识符。
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// 获取或设置是否为重复投递。
    /// </summary>
    public bool Duplicate { get; set; }

    /// <summary>
    /// 获取或设置是否设置保留标志。
    /// </summary>
    public bool Retain { get; set; }

    /// <summary>
    /// MessageExpiryInterval 的值。
    /// </summary>
    internal uint? MessageExpiryInterval
    {
        get
        {
            return Properties.MessageExpiryInterval;
        }

        set
        {
            Properties = new MqttPublishProperties
            {
                PayloadFormatIndicator = Properties.PayloadFormatIndicator,
                MessageExpiryInterval = value,
                ContentType = Properties.ContentType,
                ResponseTopic = Properties.ResponseTopic,
                CorrelationData = Properties.CorrelationData,
                UserProperties = Properties.UserProperties
            };
        }
    }

    /// <summary>
    /// SubscriptionIdentifier 的值。
    /// </summary>
    internal uint? SubscriptionIdentifier
    {
        get
        {
            return SubscriptionIdentifiers.Count == 0
                ? null
                : SubscriptionIdentifiers[0];
        }

        set
        {
            SubscriptionIdentifiers = value.HasValue
                ? [value.Value]
                : Array.Empty<uint>();
        }
    }
}
