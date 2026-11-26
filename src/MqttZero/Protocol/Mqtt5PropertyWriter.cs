using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace MqttZero.Protocol;

internal static class Mqtt5PropertyWriter
{
    public static int GetConnectPropertiesLength(MqttConnectPacket packet)
    {
        int length = 0;
        if (packet.SessionExpiryInterval.HasValue)
        {
            length += 5;
        }

        if (packet.ReceiveMaximum != ushort.MaxValue)
        {
            length += 3;
        }

        if (packet.MaximumPacketSize != uint.MaxValue)
        {
            length += 5;
        }

        if (packet.TopicAliasMaximum != 0)
        {
            length += 3;
        }

        if (!packet.RequestProblemInformation)
        {
            length += 2;
        }

        if (packet.RequestResponseInformation)
        {
            length += 2;
        }

        length += GetUserPropertiesLength(packet.UserProperties);
        if (packet.AuthenticationMethod != null)
        {
            length += GetUtf8PropertyLength(packet.AuthenticationMethod);
        }

        if (packet.AuthenticationData.HasValue)
        {
            length += 3 + packet.AuthenticationData.Value.Length;
        }

        return length;
    }

    public static void WriteConnectProperties(
        MqttConnectPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetConnectPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (packet.SessionExpiryInterval.HasValue)
        {
            WriteUInt32(
                writer,
                Mqtt5PropertyId.SessionExpiryInterval,
                packet.SessionExpiryInterval.Value);
        }

        if (packet.ReceiveMaximum != ushort.MaxValue)
        {
            WriteUInt16(writer, Mqtt5PropertyId.ReceiveMaximum, packet.ReceiveMaximum);
        }

        if (packet.MaximumPacketSize != uint.MaxValue)
        {
            WriteUInt32(
                writer,
                Mqtt5PropertyId.MaximumPacketSize,
                packet.MaximumPacketSize);
        }

        if (packet.TopicAliasMaximum != 0)
        {
            WriteUInt16(writer, Mqtt5PropertyId.TopicAliasMaximum, packet.TopicAliasMaximum);
        }

        if (!packet.RequestProblemInformation)
        {
            WriteBoolean(writer, Mqtt5PropertyId.RequestProblemInformation, false);
        }

        if (packet.RequestResponseInformation)
        {
            WriteBoolean(writer, Mqtt5PropertyId.RequestResponseInformation, true);
        }

        WriteUserProperties(writer, packet.UserProperties);
        if (packet.AuthenticationMethod != null)
        {
            WriteString(writer, Mqtt5PropertyId.AuthenticationMethod, packet.AuthenticationMethod);
        }

        if (packet.AuthenticationData.HasValue)
        {
            WriteBinary(writer, Mqtt5PropertyId.AuthenticationData, packet.AuthenticationData.Value.Span);
        }
    }

    public static int GetWillPropertiesLength(MqttConnectPacket packet)
    {
        MqttPublishProperties properties = packet.WillProperties;
        int length = GetMessagePropertiesLength(
            properties,
            Array.Empty<uint>(),
            topicAlias: null);
        if (packet.WillDelayInterval != 0)
        {
            length += 5;
        }

        return length;
    }

    public static void WriteWillProperties(
        MqttConnectPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetWillPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (packet.WillDelayInterval != 0)
        {
            WriteUInt32(writer, Mqtt5PropertyId.WillDelayInterval, packet.WillDelayInterval);
        }

        WriteMessageProperties(
            writer,
            packet.WillProperties,
            Array.Empty<uint>(),
            topicAlias: null);
    }

    public static int GetPublishPropertiesLength(MqttPublishPacket packet)
    {
        return GetMessagePropertiesLength(
            packet.Properties,
            packet.SubscriptionIdentifiers,
            packet.TopicAlias);
    }

    public static void WritePublishProperties(
        MqttPublishPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetPublishPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        WriteMessageProperties(
            writer,
            packet.Properties,
            packet.SubscriptionIdentifiers,
            packet.TopicAlias);
    }

    public static int GetConnAckPropertiesLength(MqttConnAckPacket packet)
    {
        int length = 0;
        if (packet.TopicAliasMaximum.HasValue)
        {
            length += 3;
        }

        if (packet.ReceiveMaximum.HasValue)
        {
            length += 3;
        }

        if (packet.MaximumPacketSize.HasValue)
        {
            length += 5;
        }

        if (packet.SessionExpiryInterval.HasValue)
        {
            length += 5;
        }

        if (!string.IsNullOrEmpty(packet.AssignedClientIdentifier))
        {
            length += 3 + Encoding.UTF8.GetByteCount(packet.AssignedClientIdentifier);
        }

        if (!string.IsNullOrEmpty(packet.ReasonString))
        {
            length += GetUtf8PropertyLength(packet.ReasonString);
        }

        if (packet.MaximumQos.HasValue)
        {
            if (packet.MaximumQos.Value == MqttQualityOfServiceLevel.ExactlyOnce)
            {
                throw new ArgumentException("MQTT Maximum QoS must be 0 or 1 when present.");
            }

            length += 2;
        }

        length += GetAvailabilityPropertyLength(packet.RetainAvailable);
        length += GetAvailabilityPropertyLength(packet.WildcardSubscriptionAvailable);
        length += GetAvailabilityPropertyLength(packet.SubscriptionIdentifierAvailable);
        length += GetAvailabilityPropertyLength(packet.SharedSubscriptionAvailable);
        if (packet.ServerKeepAlive.HasValue)
        {
            length += 3;
        }

        if (packet.ResponseInformation != null)
        {
            length += GetUtf8PropertyLength(packet.ResponseInformation);
        }

        if (packet.ServerReference != null)
        {
            length += GetUtf8PropertyLength(packet.ServerReference);
        }

        if (packet.AuthenticationMethod != null)
        {
            length += GetUtf8PropertyLength(packet.AuthenticationMethod);
        }

        if (packet.AuthenticationData.HasValue)
        {
            if (packet.AuthenticationMethod == null)
            {
                throw new ArgumentException(
                    "MQTT authentication data requires an authentication method.");
            }

            length += 3 + packet.AuthenticationData.Value.Length;
        }

        length += GetUserPropertiesLength(packet.UserProperties);
        return length;
    }

    public static void WriteConnAckProperties(
        MqttConnAckPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetConnAckPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (packet.TopicAliasMaximum.HasValue)
        {
            WriteUInt16(writer, Mqtt5PropertyId.TopicAliasMaximum, packet.TopicAliasMaximum.Value);
        }

        if (packet.ReceiveMaximum.HasValue)
        {
            WriteUInt16(writer, Mqtt5PropertyId.ReceiveMaximum, packet.ReceiveMaximum.Value);
        }

        if (packet.MaximumPacketSize.HasValue)
        {
            WriteUInt32(writer, Mqtt5PropertyId.MaximumPacketSize, packet.MaximumPacketSize.Value);
        }

        if (packet.SessionExpiryInterval.HasValue)
        {
            WriteUInt32(
                writer,
                Mqtt5PropertyId.SessionExpiryInterval,
                packet.SessionExpiryInterval.Value);
        }

        if (!string.IsNullOrEmpty(packet.AssignedClientIdentifier))
        {
            WriteString(
                writer,
                Mqtt5PropertyId.AssignedClientIdentifier,
                packet.AssignedClientIdentifier);
        }

        if (!string.IsNullOrEmpty(packet.ReasonString))
        {
            WriteString(writer, Mqtt5PropertyId.ReasonString, packet.ReasonString);
        }

        if (packet.MaximumQos.HasValue)
        {
            WriteByteProperty(writer, Mqtt5PropertyId.MaximumQos, (byte)packet.MaximumQos.Value);
        }

        WriteAvailabilityProperty(writer, Mqtt5PropertyId.RetainAvailable, packet.RetainAvailable);
        WriteAvailabilityProperty(
            writer,
            Mqtt5PropertyId.WildcardSubscriptionAvailable,
            packet.WildcardSubscriptionAvailable);
        WriteAvailabilityProperty(
            writer,
            Mqtt5PropertyId.SubscriptionIdentifierAvailable,
            packet.SubscriptionIdentifierAvailable);
        WriteAvailabilityProperty(
            writer,
            Mqtt5PropertyId.SharedSubscriptionAvailable,
            packet.SharedSubscriptionAvailable);
        if (packet.ServerKeepAlive.HasValue)
        {
            WriteUInt16(writer, Mqtt5PropertyId.ServerKeepAlive, packet.ServerKeepAlive.Value);
        }

        if (packet.ResponseInformation != null)
        {
            WriteString(writer, Mqtt5PropertyId.ResponseInformation, packet.ResponseInformation);
        }

        if (packet.ServerReference != null)
        {
            WriteString(writer, Mqtt5PropertyId.ServerReference, packet.ServerReference);
        }

        if (packet.AuthenticationMethod != null)
        {
            WriteString(writer, Mqtt5PropertyId.AuthenticationMethod, packet.AuthenticationMethod);
        }

        if (packet.AuthenticationData.HasValue)
        {
            WriteBinary(writer, Mqtt5PropertyId.AuthenticationData, packet.AuthenticationData.Value.Span);
        }

        WriteUserProperties(writer, packet.UserProperties);
    }

    public static int GetDisconnectPropertiesLength(MqttDisconnectPacket packet)
    {
        int length = packet.SessionExpiryInterval.HasValue ? 5 : 0;
        if (packet.ReasonString != null)
        {
            length += GetUtf8PropertyLength(packet.ReasonString);
        }

        if (packet.ServerReference != null)
        {
            length += GetUtf8PropertyLength(packet.ServerReference);
        }

        return length + GetUserPropertiesLength(packet.UserProperties);
    }

    public static void WriteDisconnectProperties(
        MqttDisconnectPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetDisconnectPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (packet.SessionExpiryInterval.HasValue)
        {
            WriteUInt32(
                writer,
                Mqtt5PropertyId.SessionExpiryInterval,
                packet.SessionExpiryInterval.Value);
        }

        if (packet.ReasonString != null)
        {
            WriteString(writer, Mqtt5PropertyId.ReasonString, packet.ReasonString);
        }

        if (packet.ServerReference != null)
        {
            WriteString(writer, Mqtt5PropertyId.ServerReference, packet.ServerReference);
        }

        WriteUserProperties(writer, packet.UserProperties);
    }

    public static int GetSubscribePropertiesLength(MqttSubscribePacket packet)
    {
        int length = 0;
        if (packet.SubscriptionIdentifier.HasValue)
        {
            if (packet.SubscriptionIdentifier.Value == 0)
            {
                throw new ArgumentException("MQTT subscription identifiers cannot be zero.");
            }

            length += 1 + MqttZeroWireFormat.GetVariableByteIntegerSize(packet.SubscriptionIdentifier.Value);
        }

        return length + GetUserPropertiesLength(packet.UserProperties);
    }

    public static void WriteSubscribeProperties(
        MqttSubscribePacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetSubscribePropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (packet.SubscriptionIdentifier.HasValue)
        {
            WriteByte(writer, Mqtt5PropertyId.SubscriptionIdentifier);
            WriteVariableByteInteger(writer, packet.SubscriptionIdentifier.Value);
        }

        WriteUserProperties(writer, packet.UserProperties);
    }

    public static int GetUnsubscribePropertiesLength(MqttUnsubscribePacket packet)
    {
        return GetUserPropertiesLength(packet.UserProperties);
    }

    public static void WriteUnsubscribeProperties(
        MqttUnsubscribePacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetUnsubscribePropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        WriteUserProperties(writer, packet.UserProperties);
    }

    public static int GetReasonPropertiesLength(
        string? reasonString,
        IReadOnlyList<MqttUserProperty> userProperties)
    {
        int length = reasonString == null ? 0 : GetUtf8PropertyLength(reasonString);
        return length + GetUserPropertiesLength(userProperties);
    }

    public static void WriteReasonProperties(
        IBufferWriter<byte> writer,
        string? reasonString,
        IReadOnlyList<MqttUserProperty> userProperties)
    {
        int propertyLength = GetReasonPropertiesLength(reasonString, userProperties);
        WritePropertyLength(writer, propertyLength);
        if (reasonString != null)
        {
            WriteString(writer, Mqtt5PropertyId.ReasonString, reasonString);
        }

        WriteUserProperties(writer, userProperties);
    }

    private static int GetMessagePropertiesLength(
        MqttPublishProperties properties,
        IReadOnlyList<uint> subscriptionIdentifiers,
        ushort? topicAlias)
    {
        ArgumentNullException.ThrowIfNull(properties);
        int length = 0;
        if (properties.PayloadFormatIndicator.HasValue)
        {
            length += 2;
        }

        if (properties.MessageExpiryInterval.HasValue)
        {
            length += 5;
        }

        if (properties.ContentType != null)
        {
            length += GetUtf8PropertyLength(properties.ContentType);
        }

        if (properties.ResponseTopic != null)
        {
            length += GetUtf8PropertyLength(properties.ResponseTopic);
        }

        if (properties.CorrelationData.HasValue)
        {
            length += 3 + properties.CorrelationData.Value.Length;
        }

        foreach (uint subscriptionIdentifier in subscriptionIdentifiers)
        {
            if (subscriptionIdentifier == 0)
            {
                throw new ArgumentException("MQTT subscription identifiers cannot be zero.");
            }

            length += 1 + MqttZeroWireFormat.GetVariableByteIntegerSize(subscriptionIdentifier);
        }

        if (topicAlias == 0)
        {
            throw new ArgumentException("MQTT topic aliases cannot be zero.");
        }

        if (topicAlias.HasValue)
        {
            length += 3;
        }

        return length + GetUserPropertiesLength(properties.UserProperties);
    }

    private static void WriteMessageProperties(
        IBufferWriter<byte> writer,
        MqttPublishProperties properties,
        IReadOnlyList<uint> subscriptionIdentifiers,
        ushort? topicAlias)
    {
        if (properties.PayloadFormatIndicator.HasValue)
        {
            if (properties.PayloadFormatIndicator.Value >
                MqttPayloadFormatIndicator.Utf8)
            {
                throw new ArgumentException("The MQTT payload format indicator is invalid.");
            }

            WriteBoolean(
                writer,
                Mqtt5PropertyId.PayloadFormatIndicator,
                properties.PayloadFormatIndicator.Value ==
                    MqttPayloadFormatIndicator.Utf8);
        }

        if (properties.MessageExpiryInterval.HasValue)
        {
            WriteUInt32(
                writer,
                Mqtt5PropertyId.MessageExpiryInterval,
                properties.MessageExpiryInterval.Value);
        }

        if (properties.ContentType != null)
        {
            WriteString(writer, Mqtt5PropertyId.ContentType, properties.ContentType);
        }

        if (properties.ResponseTopic != null)
        {
            ValidateTopicName(properties.ResponseTopic);
            WriteString(writer, Mqtt5PropertyId.ResponseTopic, properties.ResponseTopic);
        }

        if (properties.CorrelationData.HasValue)
        {
            WriteBinary(writer, Mqtt5PropertyId.CorrelationData, properties.CorrelationData.Value.Span);
        }

        foreach (uint subscriptionIdentifier in subscriptionIdentifiers)
        {
            if (subscriptionIdentifier == 0)
            {
                throw new ArgumentException("MQTT subscription identifiers cannot be zero.");
            }

            WriteByte(writer, Mqtt5PropertyId.SubscriptionIdentifier);
            WriteVariableByteInteger(writer, subscriptionIdentifier);
        }

        if (topicAlias == 0)
        {
            throw new ArgumentException("MQTT topic aliases cannot be zero.");
        }

        if (topicAlias.HasValue)
        {
            WriteUInt16(writer, Mqtt5PropertyId.TopicAlias, topicAlias.Value);
        }

        WriteUserProperties(writer, properties.UserProperties);
    }

    private static int GetUserPropertiesLength(
        IReadOnlyList<MqttUserProperty>? userProperties)
    {
        if (userProperties == null)
        {
            return 0;
        }

        int length = 0;
        foreach (MqttUserProperty userProperty in userProperties)
        {
            ArgumentNullException.ThrowIfNull(userProperty);
            length += 1 +
                2 +
                Encoding.UTF8.GetByteCount(userProperty.Name) +
                2 +
                Encoding.UTF8.GetByteCount(userProperty.Value);
        }

        return length;
    }

    private static void WriteUserProperties(
        IBufferWriter<byte> writer,
        IReadOnlyList<MqttUserProperty>? userProperties)
    {
        if (userProperties == null)
        {
            return;
        }

        foreach (MqttUserProperty userProperty in userProperties)
        {
            ArgumentNullException.ThrowIfNull(userProperty);
            WriteUserProperty(writer, userProperty);
        }
    }

    private static void ValidateTopicName(string topic)
    {
        byte[] encoded = Encoding.UTF8.GetBytes(topic);
        if (!MqttZeroProtocolValidator.IsValidTopicName(
                new ReadOnlySequence<byte>(encoded)))
        {
            throw new ArgumentException("The MQTT response topic is invalid.");
        }
    }

    private static void WritePropertyLength(IBufferWriter<byte> writer, int length)
    {
        Span<byte> destination = writer.GetSpan(4);
        int count = MqttZeroWireFormat.WriteVariableByteInteger(
            destination,
            length);
        writer.Advance(count);
    }

    private static void WriteSequence(
        IBufferWriter<byte> writer,
        ReadOnlySequence<byte> value)
    {
        if (value.IsSingleSegment)
        {
            WriteBytes(writer, value.FirstSpan);
            return;
        }

        foreach (ReadOnlyMemory<byte> segment in value)
        {
            WriteBytes(writer, segment.Span);
        }
    }

    private static void WriteBytes(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        Span<byte> destination = writer.GetSpan(value.Length);
        value.CopyTo(destination);
        writer.Advance(value.Length);
    }

    private static void WriteString(IBufferWriter<byte> writer, byte identifier, string value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        Span<byte> destination = writer.GetSpan(byteCount + 3);
        destination[0] = identifier;
        BinaryPrimitives.WriteUInt16BigEndian(destination[1..], (ushort)byteCount);
        Encoding.UTF8.GetBytes(value, destination[3..]);
        writer.Advance(byteCount + 3);
    }

    private static void WriteBinary(
        IBufferWriter<byte> writer,
        byte identifier,
        ReadOnlySpan<byte> value)
    {
        Span<byte> destination = writer.GetSpan(value.Length + 3);
        destination[0] = identifier;
        BinaryPrimitives.WriteUInt16BigEndian(destination[1..], (ushort)value.Length);
        value.CopyTo(destination[3..]);
        writer.Advance(value.Length + 3);
    }

    private static void WriteUserProperty(
        IBufferWriter<byte> writer,
        MqttUserProperty property)
    {
        int nameLength = Encoding.UTF8.GetByteCount(property.Name);
        int valueLength = Encoding.UTF8.GetByteCount(property.Value);
        Span<byte> destination = writer.GetSpan(1 + 4 + nameLength + valueLength);
        destination[0] = Mqtt5PropertyId.UserProperty;
        BinaryPrimitives.WriteUInt16BigEndian(destination[1..], (ushort)nameLength);
        Encoding.UTF8.GetBytes(property.Name, destination[3..]);
        int valueOffset = 3 + nameLength;
        BinaryPrimitives.WriteUInt16BigEndian(
            destination[valueOffset..],
            (ushort)valueLength);
        Encoding.UTF8.GetBytes(property.Value, destination[(valueOffset + 2)..]);
        writer.Advance(1 + 4 + nameLength + valueLength);
    }

    private static int GetUtf8PropertyLength(string value)
    {
        return 3 + Encoding.UTF8.GetByteCount(value);
    }

    private static void WriteUInt16(
        IBufferWriter<byte> writer,
        byte identifier,
        ushort value)
    {
        Span<byte> destination = writer.GetSpan(3);
        destination[0] = identifier;
        BinaryPrimitives.WriteUInt16BigEndian(destination[1..], value);
        writer.Advance(3);
    }

    private static void WriteUInt32(
        IBufferWriter<byte> writer,
        byte identifier,
        uint value)
    {
        Span<byte> destination = writer.GetSpan(5);
        destination[0] = identifier;
        BinaryPrimitives.WriteUInt32BigEndian(destination[1..], value);
        writer.Advance(5);
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        Span<byte> destination = writer.GetSpan(1);
        destination[0] = value;
        writer.Advance(1);
    }

    private static void WriteBoolean(
        IBufferWriter<byte> writer,
        byte identifier,
        bool value)
    {
        Span<byte> destination = writer.GetSpan(2);
        destination[0] = identifier;
        destination[1] = value ? (byte)1 : (byte)0;
        writer.Advance(2);
    }

    private static void WriteByteProperty(
        IBufferWriter<byte> writer,
        byte identifier,
        byte value)
    {
        Span<byte> destination = writer.GetSpan(2);
        destination[0] = identifier;
        destination[1] = value;
        writer.Advance(2);
    }

    private static int GetAvailabilityPropertyLength(bool? value)
    {
        return value == false ? 2 : 0;
    }

    private static void WriteAvailabilityProperty(
        IBufferWriter<byte> writer,
        byte identifier,
        bool? value)
    {
        if (value == false)
        {
            WriteBoolean(writer, identifier, false);
        }
    }

    private static void WriteVariableByteInteger(IBufferWriter<byte> writer, uint value)
    {
        Span<byte> destination = writer.GetSpan(4);
        int count = MqttZeroWireFormat.WriteVariableByteInteger(destination, (int)value);
        writer.Advance(count);
    }

}
