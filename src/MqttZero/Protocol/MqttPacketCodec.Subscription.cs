using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 报文的统一解码与编码入口，按报文族拆成多个分部文件。
/// </summary>
public static partial class MqttPacketCodec
{
    /// <summary>
    /// 尝试Decode订阅。
    /// </summary>
    /// <param name="firstByte">第一个字节。</param>
    /// <param name="reader">读取器。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryDecodeSubscribe(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0x02 ||
            !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort packetIdentifier) ||
            packetIdentifier == 0 ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        List<MqttSubscriptionRequest> subscriptions = [];
        while (reader.Remaining > 0)
        {
            if (!TryReadUtf8(ref reader, out ReadOnlySequence<byte> topicFilter) ||
                !reader.TryRead(out byte options) ||
                !MqttZeroProtocolValidator.IsValidTopicFilter(topicFilter))
            {
                return false;
            }

            MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)(options & 0x03);
            byte encodedRetainHandling = (byte)((options >> 4) & 0x03);
            if ((byte)qos > (byte)MqttQualityOfServiceLevel.ExactlyOnce ||
                encodedRetainHandling == 3)
            {
                return false;
            }

            subscriptions.Add(new MqttSubscriptionRequest
            {
                TopicFilter = topicFilter,
                QualityOfServiceLevel = qos,
                NoLocal = (options & 0x04) != 0,
                RetainAsPublished = (options & 0x08) != 0,
                RetainHandling = (MqttRetainHandling)encodedRetainHandling
            });
        }

        packet = new MqttSubscribePacket
        {
            PacketIdentifier = packetIdentifier,
            Subscriptions = subscriptions
        };
        if (!Mqtt5PropertyReader.TryReadSubscribe(properties, (MqttSubscribePacket)packet))
        {
            packet = null;
            return false;
        }

        return subscriptions.Count > 0;
    }

    /// <summary>
    /// 尝试Decode退订。
    /// </summary>
    /// <param name="firstByte">第一个字节。</param>
    /// <param name="reader">读取器。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryDecodeUnsubscribe(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0x02 ||
            !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort packetIdentifier) ||
            packetIdentifier == 0 ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        List<ReadOnlySequence<byte>> topicFilters = [];
        while (reader.Remaining > 0)
        {
            if (!TryReadUtf8(ref reader, out ReadOnlySequence<byte> topicFilter) ||
                !MqttZeroProtocolValidator.IsValidTopicFilter(topicFilter))
            {
                return false;
            }

            topicFilters.Add(topicFilter);
        }

        packet = new MqttUnsubscribePacket
        {
            PacketIdentifier = packetIdentifier,
            TopicFilters = topicFilters
        };
        return topicFilters.Count > 0 &&
            Mqtt5PropertyReader.TryReadUnsubscribe(
                properties,
                (MqttUnsubscribePacket)packet);
    }

    /// <summary>
    /// 尝试DecodeSub确认。
    /// </summary>
    /// <param name="firstByte">第一个字节。</param>
    /// <param name="reader">读取器。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryDecodeSubAck(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort packetIdentifier) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        ReadOnlyMemory<byte> reasonCodes = reader.Sequence
            .Slice(reader.Position, reader.Remaining)
            .ToArray();
        reader.Advance(reader.Remaining);
        packet = new MqttSubAckPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCodes = reasonCodes
        };
        bool validReasons = true;
        foreach (byte reasonCode in reasonCodes.Span)
        {
            if (!MqttZeroProtocolValidator.IsValidSubAckReasonCode(reasonCode))
            {
                validReasons = false;
                break;
            }
        }

        return packetIdentifier != 0 &&
            reasonCodes.Length > 0 &&
            validReasons &&
            Mqtt5PropertyReader.TryReadReasonProperties(
                properties,
                out string? reasonString,
                out IReadOnlyList<MqttUserProperty> userProperties) &&
            AssignSubAckProperties(
                (MqttSubAckPacket)packet,
                reasonString,
                userProperties);
    }

    /// <summary>
    /// 尝试DecodeUnsub确认。
    /// </summary>
    /// <param name="firstByte">第一个字节。</param>
    /// <param name="reader">读取器。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryDecodeUnsubAck(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort packetIdentifier) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        ReadOnlyMemory<byte> reasonCodes = reader.UnreadSequence.ToArray();
        reader.Advance(reader.Remaining);
        packet = new MqttUnsubAckPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCodes = reasonCodes
        };
        bool validReasons = true;
        foreach (byte reasonCode in reasonCodes.Span)
        {
            if (!MqttZeroProtocolValidator.IsValidUnsubAckReasonCode(reasonCode))
            {
                validReasons = false;
                break;
            }
        }

        return packetIdentifier != 0 &&
            reasonCodes.Length > 0 &&
            validReasons &&
            Mqtt5PropertyReader.TryReadReasonProperties(
                properties,
                out string? reasonString,
                out IReadOnlyList<MqttUserProperty> userProperties) &&
            AssignUnsubAckProperties(
                (MqttUnsubAckPacket)packet,
                reasonString,
                userProperties);
    }

    /// <summary>
    /// 编码订阅。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="writer">写入器。</param>
    private static void EncodeSubscribe(MqttSubscribePacket packet, IBufferWriter<byte> writer)
    {
        if (packet.PacketIdentifier == 0 || packet.Subscriptions.Count == 0)
        {
            throw new ArgumentException("The MQTT SUBSCRIBE packet is invalid.", nameof(packet));
        }

        int propertiesLength = Mqtt5PropertyWriter.GetSubscribePropertiesLength(packet);
        int bodyLength = 2 +
            MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength;
        foreach (MqttSubscriptionRequest subscription in packet.Subscriptions)
        {
            if (!MqttZeroProtocolValidator.IsValidTopicFilter(subscription.TopicFilter))
            {
                throw new ArgumentException(
                    "The MQTT subscription topic filter is invalid.",
                    nameof(packet));
            }

            bodyLength += 2 + (int)subscription.TopicFilter.Length + 1;
        }

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.Subscribe);
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);
        Mqtt5PropertyWriter.WriteSubscribeProperties(packet, writer);

        foreach (MqttSubscriptionRequest subscription in packet.Subscriptions)
        {
            WriteUtf8(writer, subscription.TopicFilter);
            byte options = (byte)subscription.QualityOfServiceLevel;
            if (subscription.NoLocal)
            {
                options |= 0x04;
            }

            if (subscription.RetainAsPublished)
            {
                options |= 0x08;
            }

            options |= (byte)((byte)subscription.RetainHandling << 4);
            WriteByte(writer, options);
        }
    }

    /// <summary>
    /// 编码Sub确认。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="writer">写入器。</param>
    private static void EncodeSubAck(MqttSubAckPacket packet, IBufferWriter<byte> writer)
    {
        if (packet.PacketIdentifier == 0 || packet.ReasonCodes.IsEmpty)
        {
            throw new ArgumentException("The MQTT SUBACK packet is invalid.", nameof(packet));
        }

        foreach (byte reasonCode in packet.ReasonCodes.Span)
        {
            if (!MqttZeroProtocolValidator.IsValidSubAckReasonCode(reasonCode))
            {
                throw new ArgumentException(
                    "The MQTT SUBACK reason code is invalid.",
                    nameof(packet));
            }
        }

        int propertiesLength = Mqtt5PropertyWriter.GetReasonPropertiesLength(
            packet.ReasonString,
            packet.UserProperties);
        int bodyLength = 2 +
            MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength +
            packet.ReasonCodes.Length;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.SubAck);
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);
        Mqtt5PropertyWriter.WriteReasonProperties(
            writer,
            packet.ReasonString,
            packet.UserProperties);

        Span<byte> reasonCodes = writer.GetSpan(packet.ReasonCodes.Length);
        packet.ReasonCodes.Span.CopyTo(reasonCodes);
        writer.Advance(packet.ReasonCodes.Length);
    }

    /// <summary>
    /// 编码退订。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="writer">写入器。</param>
    private static void EncodeUnsubscribe(MqttUnsubscribePacket packet, IBufferWriter<byte> writer)
    {
        if (packet.PacketIdentifier == 0 || packet.TopicFilters.Count == 0)
        {
            throw new ArgumentException(
                "The MQTT UNSUBSCRIBE packet is invalid.",
                nameof(packet));
        }

        int propertiesLength = Mqtt5PropertyWriter.GetUnsubscribePropertiesLength(packet);
        int bodyLength = 2 +
            MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength;
        foreach (ReadOnlySequence<byte> topicFilter in packet.TopicFilters)
        {
            if (!MqttZeroProtocolValidator.IsValidTopicFilter(topicFilter))
            {
                throw new ArgumentException(
                    "The MQTT unsubscribe topic filter is invalid.",
                    nameof(packet));
            }

            bodyLength += 2 + (int)topicFilter.Length;
        }

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.Unsubscribe);
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);
        Mqtt5PropertyWriter.WriteUnsubscribeProperties(packet, writer);
        foreach (ReadOnlySequence<byte> topicFilter in packet.TopicFilters)
        {
            WriteUtf8(writer, topicFilter);
        }
    }

    /// <summary>
    /// 编码Unsub确认。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="writer">写入器。</param>
    private static void EncodeUnsubAck(MqttUnsubAckPacket packet, IBufferWriter<byte> writer)
    {
        if (packet.PacketIdentifier == 0 || packet.ReasonCodes.IsEmpty)
        {
            throw new ArgumentException("The MQTT UNSUBACK packet is invalid.", nameof(packet));
        }

        foreach (byte reasonCode in packet.ReasonCodes.Span)
        {
            if (!MqttZeroProtocolValidator.IsValidUnsubAckReasonCode(reasonCode))
            {
                throw new ArgumentException(
                    "The MQTT UNSUBACK reason code is invalid.",
                    nameof(packet));
            }
        }

        int propertiesLength = Mqtt5PropertyWriter.GetReasonPropertiesLength(
            packet.ReasonString,
            packet.UserProperties);
        int bodyLength = 2 +
            MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength +
            packet.ReasonCodes.Length;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.UnsubAck);
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);
        Mqtt5PropertyWriter.WriteReasonProperties(
            writer,
            packet.ReasonString,
            packet.UserProperties);
        Span<byte> reasonCodes = writer.GetSpan(packet.ReasonCodes.Length);
        packet.ReasonCodes.Span.CopyTo(reasonCodes);
        writer.Advance(packet.ReasonCodes.Length);
    }

    /// <summary>
    /// AssignSub确认属性。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="reasonString">原因字符串。</param>
    /// <param name="userProperties">用户属性。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool AssignSubAckProperties(
        MqttSubAckPacket packet,
        string? reasonString,
        IReadOnlyList<MqttUserProperty> userProperties)
    {
        packet.ReasonString = reasonString;
        packet.UserProperties = userProperties;
        return true;
    }

    /// <summary>
    /// AssignUnsub确认属性。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="reasonString">原因字符串。</param>
    /// <param name="userProperties">用户属性。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool AssignUnsubAckProperties(
        MqttUnsubAckPacket packet,
        string? reasonString,
        IReadOnlyList<MqttUserProperty> userProperties)
    {
        packet.ReasonString = reasonString;
        packet.UserProperties = userProperties;
        return true;
    }

}
