using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 5 PUBLISH 发布报文。
/// </summary>
public sealed class MqttPublishPacket : MqttPacket
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
    /// Gets or sets the UTF-8 topic name.
    /// </summary>
    public ReadOnlySequence<byte> TopicName { get; set; }

    /// <summary>
    /// Gets or sets the end-to-end publish properties.
    /// </summary>
    public MqttPublishProperties Properties { get; set; } = MqttPublishProperties.Empty;

    /// <summary>
    /// Gets or sets the subscription identifiers attached to this delivery.
    /// </summary>
    public IReadOnlyList<uint> SubscriptionIdentifiers { get; set; } = Array.Empty<uint>();

    /// <summary>
    /// Gets or sets the MQTT 5 topic alias.
    /// </summary>
    public ushort? TopicAlias { get; set; }

    /// <summary>
    /// Gets or sets the payload sequence.
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; set; }

    /// <summary>
    /// Gets or sets the delivery quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets the QoS packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this is a duplicate delivery.
    /// </summary>
    public bool Duplicate { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the retained flag is set.
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
