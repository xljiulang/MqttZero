using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// PUBLISH 报文各段数据的只读视图。
/// </summary>
internal readonly ref struct MqttZeroPublishView
{
    /// <summary>
    /// 初始化 MqttZeroPublishView 的新实例。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="properties">属性。</param>
    /// <param name="payload">负载。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <param name="retain">保留。</param>
    /// <param name="messageExpiryInterval">消息Expiry间隔。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="topicAlias">主题别名。</param>
    public MqttZeroPublishView(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        ushort packetIdentifier,
        bool duplicate,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        ushort? topicAlias)
        : this(
            topic,
            properties,
            payload,
            qualityOfServiceLevel,
            packetIdentifier,
            duplicate,
            retain,
            messageExpiryInterval,
            subscriptionIdentifier,
            ReadOnlyMemory<uint>.Empty,
            topicAlias)
    {
    }

    /// <summary>
    /// 初始化 MqttZeroPublishView 的新实例。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="properties">属性。</param>
    /// <param name="payload">负载。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="duplicate">对应的 duplicate。</param>
    /// <param name="retain">保留。</param>
    /// <param name="messageExpiryInterval">消息Expiry间隔。</param>
    /// <param name="subscriptionIdentifier">订阅标识符。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="topicAlias">主题别名。</param>
    public MqttZeroPublishView(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        ushort packetIdentifier,
        bool duplicate,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        ReadOnlyMemory<uint> subscriptionIdentifiers,
        ushort? topicAlias)
    {
        Topic = topic;
        Properties = properties;
        Payload = payload;
        QualityOfServiceLevel = qualityOfServiceLevel;
        PacketIdentifier = packetIdentifier;
        Duplicate = duplicate;
        Retain = retain;
        MessageExpiryInterval = messageExpiryInterval;
        SubscriptionIdentifier = subscriptionIdentifier;
        SubscriptionIdentifiers = subscriptionIdentifiers;
        TopicAlias = topicAlias;
    }

    /// <summary>
    /// 主题。
    /// </summary>
    public ReadOnlySequence<byte> Topic { get; }

    /// <summary>
    /// 属性。
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; }

    /// <summary>
    /// 负载。
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; }

    /// <summary>
    /// 服务质量ServiceLevel。
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    /// <summary>
    /// 报文标识符。
    /// </summary>
    public ushort PacketIdentifier { get; }

    /// <summary>
    /// Duplicate 的值。
    /// </summary>
    public bool Duplicate { get; }

    /// <summary>
    /// 保留。
    /// </summary>
    public bool Retain { get; }

    /// <summary>
    /// 消息Expiry间隔。
    /// </summary>
    public uint? MessageExpiryInterval { get; }

    /// <summary>
    /// 订阅标识符。
    /// </summary>
    public uint? SubscriptionIdentifier { get; }

    /// <summary>
    /// 订阅标识符。
    /// </summary>
    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; }

    /// <summary>
    /// 主题别名。
    /// </summary>
    public ushort? TopicAlias { get; }
}
