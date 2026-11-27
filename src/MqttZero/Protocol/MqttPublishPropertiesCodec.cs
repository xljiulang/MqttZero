using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// PUBLISH 属性的编解码实现。
/// </summary>
internal static class MqttPublishPropertiesCodec
{
    /// <summary>
    /// 尝试Decode。
    /// </summary>
    /// <param name="encodedProperties">encoded属性。</param>
    /// <param name="properties">属性。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="topicAlias">主题别名。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    public static bool TryDecode(
        ReadOnlySequence<byte> encodedProperties,
        out MqttPublishProperties properties,
        out IReadOnlyList<uint> subscriptionIdentifiers,
        out ushort? topicAlias)
    {
        if (encodedProperties.IsEmpty)
        {
            properties = MqttPublishProperties.Empty;
            subscriptionIdentifiers = Array.Empty<uint>();
            topicAlias = null;
            return true;
        }

        MqttPublishPacket packet = new();
        if (!Mqtt5PropertyReader.TryReadPublish(encodedProperties, packet))
        {
            properties = MqttPublishProperties.Empty;
            subscriptionIdentifiers = Array.Empty<uint>();
            topicAlias = null;
            return false;
        }

        properties = packet.Properties;
        subscriptionIdentifiers = packet.SubscriptionIdentifiers;
        topicAlias = packet.TopicAlias;
        return true;
    }

    /// <summary>
    /// 编码。
    /// </summary>
    /// <param name="properties">属性。</param>
    /// <param name="subscriptionIdentifiers">订阅标识符。</param>
    /// <param name="topicAlias">主题别名。</param>
    /// <returns>只读字节序列。</returns>
    public static ReadOnlySequence<byte> Encode(
        MqttPublishProperties properties,
        IReadOnlyList<uint>? subscriptionIdentifiers = null,
        ushort? topicAlias = null)
    {
        if (ReferenceEquals(properties, MqttPublishProperties.Empty) &&
            (subscriptionIdentifiers == null || subscriptionIdentifiers.Count == 0) &&
            !topicAlias.HasValue)
        {
            return ReadOnlySequence<byte>.Empty;
        }

        MqttPublishPacket packet = new()
        {
            Properties = properties,
            SubscriptionIdentifiers = subscriptionIdentifiers ?? Array.Empty<uint>(),
            TopicAlias = topicAlias
        };
        ArrayBufferWriter<byte> writer = new(Mqtt5PropertyWriter.GetPublishPropertiesLength(packet) + 4);
        Mqtt5PropertyWriter.WritePublishProperties(packet, writer);
        int prefixLength = GetVariableByteIntegerLength(writer.WrittenSpan);
        return new ReadOnlySequence<byte>(writer.WrittenMemory[prefixLength..]);
    }

    /// <summary>
    /// 获取变长字节整数长度。
    /// </summary>
    /// <param name="value">值。</param>
    /// <returns>计算得到的数值。</returns>
    private static int GetVariableByteIntegerLength(ReadOnlySpan<byte> value)
    {
        for (int index = 0; index < Math.Min(value.Length, 4); index++)
        {
            if ((value[index] & 0x80) == 0)
            {
                return index + 1;
            }
        }

        throw new ArgumentException("The MQTT property length is invalid.", nameof(value));
    }
}
