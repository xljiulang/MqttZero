using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Storage;
using System.Buffers;

namespace MqttZero.Server.Sessions;

/// <summary>
/// 遗嘱消息。
/// </summary>
internal sealed class MqttZeroWillMessage
{
    /// <summary>
    /// 初始化 MqttZeroWillMessage 的新实例。
    /// </summary>
    /// <param name="packet">报文。</param>
    public MqttZeroWillMessage(MqttConnectPacket packet)
    {
        Topic = packet.WillTopic.ToArray();
        Payload = packet.WillPayload.ToArray();
        Properties = MqttPublishPropertiesCodec.Encode(packet.WillProperties).ToArray();
        QualityOfServiceLevel = packet.WillQualityOfServiceLevel;
        Retain = packet.WillRetain;
        DelayInterval = packet.WillDelayInterval;
        MessageExpiryInterval = packet.WillProperties.MessageExpiryInterval ?? 0;
    }

    /// <summary>
    /// 初始化 MqttZeroWillMessage 的新实例。
    /// </summary>
    /// <param name="will">遗嘱。</param>
    public MqttZeroWillMessage(MqttZeroPersistedWill will)
    {
        Topic = will.Topic.ToArray();
        Payload = will.Payload.ToArray();
        MqttConnectPacket persistedProperties = new();
        if (!Mqtt5PropertyReader.TryReadWill(
                new ReadOnlySequence<byte>(will.Properties),
                persistedProperties))
        {
            throw new InvalidOperationException("The persisted MQTT Will properties are invalid.");
        }

        Properties = MqttPublishPropertiesCodec.Encode(
            persistedProperties.WillProperties).ToArray();
        QualityOfServiceLevel = will.QualityOfServiceLevel;
        Retain = will.Retain;
        DelayInterval = will.DelayInterval;
        MessageExpiryInterval = persistedProperties.WillProperties.MessageExpiryInterval ??
            will.MessageExpiryInterval;
    }

    /// <summary>
    /// 主题。
    /// </summary>
    public byte[] Topic { get; }

    /// <summary>
    /// 负载。
    /// </summary>
    public byte[] Payload { get; }

    /// <summary>
    /// 属性。
    /// </summary>
    public byte[] Properties { get; }

    /// <summary>
    /// 服务质量ServiceLevel。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    /// <summary>
    /// 保留。
    /// </summary>
    public bool Retain { get; }

    /// <summary>
    /// Delay间隔。
    /// </summary>
    public uint DelayInterval { get; }

    /// <summary>
    /// 消息Expiry间隔。
    /// </summary>
    public uint MessageExpiryInterval { get; }

    /// <summary>
    /// 创建报文。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttPublishPacket CreatePacket()
    {
        bool decoded = MqttPublishPropertiesCodec.TryDecode(
            Properties.Length == 0
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(Properties),
            out MqttPublishProperties properties,
            out _,
            out _);
        if (!decoded)
        {
            throw new InvalidOperationException("The MQTT Will properties are invalid.");
        }

        MqttPublishPacket packet = new()
        {
            TopicName = new ReadOnlySequence<byte>(Topic),
            Payload = new ReadOnlySequence<byte>(Payload),
            Properties = properties,
            QualityOfServiceLevel = QualityOfServiceLevel,
            Retain = Retain
        };

        return packet;
    }
}
