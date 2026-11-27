using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 报文的统一解码与编码入口，按报文族拆成多个分部文件。
/// </summary>
internal static partial class MqttPacketCodec
{
    /// <summary>
    /// 尝试Decode发布。
    /// </summary>
    /// <param name="firstByte">第一个字节。</param>
    /// <param name="reader">读取器。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryDecodePublish(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)((firstByte >> 1) & 0x03);
        if ((byte)qos > (byte)MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return false;
        }

        if (!TryReadUtf8(ref reader, out ReadOnlySequence<byte> topicName))
        {
            return false;
        }

        ushort packetIdentifier = 0;
        if (qos > MqttQualityOfServiceLevel.AtMostOnce &&
            !MqttZeroWireFormat.TryReadUInt16(ref reader, out packetIdentifier))
        {
            return false;
        }

        if (!TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        ReadOnlySequence<byte> payload = reader.Sequence.Slice(reader.Position);
        reader.Advance(reader.Remaining);
        packet = new MqttPublishPacket
        {
            TopicName = topicName,
            Payload = payload,
            QualityOfServiceLevel = qos,
            PacketIdentifier = packetIdentifier,
            Duplicate = (firstByte & 0x08) != 0,
            Retain = (firstByte & 0x01) != 0
        };

        if (!Mqtt5PropertyReader.TryReadPublish(properties, (MqttPublishPacket)packet))
        {
            packet = null;
            return false;
        }

        MqttPublishPacket publishPacket = (MqttPublishPacket)packet;
        if ((qos != MqttQualityOfServiceLevel.AtMostOnce && packetIdentifier == 0) ||
            (!topicName.IsEmpty &&
                !MqttZeroProtocolValidator.IsValidTopicName(topicName)) ||
            (topicName.IsEmpty && !publishPacket.TopicAlias.HasValue))
        {
            packet = null;
            return false;
        }

        return true;
    }

    /// <summary>
    /// 编码发布。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="writer">写入器。</param>
    private static void EncodePublish(MqttPublishPacket packet, IBufferWriter<byte> writer)
    {
        if ((byte)packet.QualityOfServiceLevel >
                (byte)MqttQualityOfServiceLevel.ExactlyOnce ||
            (packet.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce &&
                packet.PacketIdentifier == 0))
        {
            throw new ArgumentException("The MQTT PUBLISH QoS or packet identifier is invalid.");
        }

        if ((!packet.TopicName.IsEmpty &&
                !MqttZeroProtocolValidator.IsValidTopicName(packet.TopicName)) ||
            (packet.TopicName.IsEmpty && !packet.TopicAlias.HasValue))
        {
            throw new ArgumentException("The MQTT PUBLISH topic name is invalid.", nameof(packet));
        }

        int propertiesLength = Mqtt5PropertyWriter.GetPublishPropertiesLength(packet);
        int bodyLength =
            2 +
            (int)packet.TopicName.Length +
            (packet.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce ? 0 : 2) +
            MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength +
            (int)packet.Payload.Length;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        byte firstByte = 0x30;
        if (packet.Duplicate)
        {
            firstByte |= 0x08;
        }

        firstByte |= (byte)((byte)packet.QualityOfServiceLevel << 1);
        if (packet.Retain)
        {
            firstByte |= 0x01;
        }

        fixedHeader[0] = firstByte;
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);

        WriteUtf8(writer, packet.TopicName);
        if (packet.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce)
        {
            WriteUInt16(writer, packet.PacketIdentifier);
        }

        Mqtt5PropertyWriter.WritePublishProperties(packet, writer);
        WriteSequence(writer, packet.Payload);
    }

}
