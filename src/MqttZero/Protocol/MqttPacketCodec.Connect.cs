using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 报文的统一解码与编码入口，按报文族拆成多个分部文件。
/// </summary>
internal static partial class MqttPacketCodec
{
    /// <summary>
    /// 尝试Decode连接。
    /// </summary>
    /// <param name="firstByte">第一个字节。</param>
    /// <param name="reader">读取器。</param>
    /// <param name="packet">报文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryDecodeConnect(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadUtf8(ref reader, out ReadOnlySequence<byte> protocolName) ||
            protocolName.Length != 4 ||
            !protocolName.FirstSpan.SequenceEqual("MQTT"u8) ||
            !reader.TryRead(out byte protocolVersion) ||
            protocolVersion != (byte)MqttProtocolVersion.V500 ||
            !reader.TryRead(out byte connectFlags) ||
            (connectFlags & 0x01) != 0 ||
            !MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort keepAliveSeconds) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties) ||
            !TryReadUtf8(ref reader, out ReadOnlySequence<byte> clientId))
        {
            return false;
        }

        bool hasUserName = (connectFlags & 0x80) != 0;
        bool hasPassword = (connectFlags & 0x40) != 0;
        bool willRetain = (connectFlags & 0x20) != 0;
        MqttQualityOfServiceLevel willQos = (MqttQualityOfServiceLevel)((connectFlags >> 3) & 0x03);
        bool hasWill = (connectFlags & 0x04) != 0;

        if ((!hasWill && (willRetain || willQos != MqttQualityOfServiceLevel.AtMostOnce)) ||
            (byte)willQos > (byte)MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return false;
        }

        ReadOnlySequence<byte> willProperties = ReadOnlySequence<byte>.Empty;
        ReadOnlySequence<byte> willTopic = ReadOnlySequence<byte>.Empty;
        ReadOnlySequence<byte> willPayload = ReadOnlySequence<byte>.Empty;
        if (hasWill)
        {
            if (!TryReadProperties(ref reader, out willProperties) ||
                !TryReadUtf8(ref reader, out willTopic) ||
                !TryReadBinary(ref reader, out willPayload))
            {
                return false;
            }
        }

        ReadOnlySequence<byte> userName = ReadOnlySequence<byte>.Empty;
        if (hasUserName && !TryReadUtf8(ref reader, out userName))
        {
            return false;
        }

        ReadOnlySequence<byte> password = ReadOnlySequence<byte>.Empty;
        if (hasPassword && !TryReadBinary(ref reader, out password))
        {
            return false;
        }

        packet = new MqttConnectPacket
        {
            ProtocolVersion = (MqttProtocolVersion)protocolVersion,
            CleanStart = (connectFlags & 0x02) != 0,
            KeepAliveSeconds = keepAliveSeconds,
            ClientId = clientId,
            HasUserName = hasUserName,
            UserName = userName,
            HasPassword = hasPassword,
            Password = password,
            HasWill = hasWill,
            WillQualityOfServiceLevel = willQos,
            WillRetain = willRetain,
            WillTopic = willTopic,
            WillPayload = willPayload
        };

        if (!Mqtt5PropertyReader.TryReadConnect(properties, (MqttConnectPacket)packet) ||
            !Mqtt5PropertyReader.TryReadWill(willProperties, (MqttConnectPacket)packet) ||
            (hasWill && !MqttZeroProtocolValidator.IsValidTopicName(willTopic)))
        {
            packet = null;
            return false;
        }

        return reader.Remaining == 0;
    }

    /// <summary>
    /// 编码连接。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="writer">写入器。</param>
    private static void EncodeConnect(MqttConnectPacket packet, IBufferWriter<byte> writer)
    {
        if (packet.HasWill &&
            !MqttZeroProtocolValidator.IsValidTopicName(packet.WillTopic))
        {
            throw new ArgumentException("The MQTT Will topic is invalid.", nameof(packet));
        }

        int connectPropertiesLength =
            Mqtt5PropertyWriter.GetConnectPropertiesLength(packet);
        int willPropertiesLength = packet.HasWill
            ? Mqtt5PropertyWriter.GetWillPropertiesLength(packet)
            : 0;
        int bodyLength = 10 +
            MqttZeroWireFormat.GetVariableByteIntegerSize(connectPropertiesLength) +
            connectPropertiesLength +
            2 +
            (int)packet.ClientId.Length;

        if (packet.HasWill)
        {
            bodyLength += MqttZeroWireFormat.GetVariableByteIntegerSize(willPropertiesLength) +
                willPropertiesLength +
                2 +
                (int)packet.WillTopic.Length +
                2 +
                (int)packet.WillPayload.Length;
        }

        if (packet.HasUserName)
        {
            bodyLength += 2 + (int)packet.UserName.Length;
        }

        if (packet.HasPassword)
        {
            bodyLength += 2 + (int)packet.Password.Length;
        }

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.Connect);
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);

        WriteUtf8(writer, "MQTT"u8);
        WriteByte(writer, (byte)packet.ProtocolVersion);
        WriteByte(writer, BuildConnectFlags(packet));
        WriteUInt16(writer, packet.KeepAliveSeconds);
        Mqtt5PropertyWriter.WriteConnectProperties(packet, writer);
        WriteUtf8(writer, packet.ClientId);

        if (packet.HasWill)
        {
            Mqtt5PropertyWriter.WriteWillProperties(packet, writer);
            WriteUtf8(writer, packet.WillTopic);
            WriteBinary(writer, packet.WillPayload);
        }

        if (packet.HasUserName)
        {
            WriteUtf8(writer, packet.UserName);
        }

        if (packet.HasPassword)
        {
            WriteBinary(writer, packet.Password);
        }
    }

    /// <summary>
    /// 编码Conn确认。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <param name="writer">写入器。</param>
    private static void EncodeConnAck(MqttConnAckPacket packet, IBufferWriter<byte> writer)
    {
        if (!MqttZeroProtocolValidator.IsValidConnAckReasonCode(packet.ReasonCode) ||
            (packet.SessionPresent && packet.ReasonCode != MqttReasonCode.Success))
        {
            throw new ArgumentException("The MQTT CONNACK packet is invalid.", nameof(packet));
        }

        int propertiesLength = Mqtt5PropertyWriter.GetConnAckPropertiesLength(packet);
        int bodyLength =
            2 +
            MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = MqttPacketHeader.GetFirstByte(MqttPacketType.ConnAck);
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteByte(writer, packet.SessionPresent ? (byte)1 : (byte)0);
        WriteByte(writer, (byte)packet.ReasonCode);
        Mqtt5PropertyWriter.WriteConnAckProperties(packet, writer);
    }

    /// <summary>
    /// 构建连接标志。
    /// </summary>
    /// <param name="packet">报文。</param>
    /// <returns>对应的结果。</returns>
    private static byte BuildConnectFlags(MqttConnectPacket packet)
    {
        byte flags = 0;
        if (packet.HasUserName)
        {
            flags |= 0x80;
        }

        if (packet.HasPassword)
        {
            flags |= 0x40;
        }

        if (packet.HasWill)
        {
            flags |= 0x04;
            flags |= (byte)((byte)packet.WillQualityOfServiceLevel << 3);
            if (packet.WillRetain)
            {
                flags |= 0x20;
            }
        }

        if (packet.CleanStart)
        {
            flags |= 0x02;
        }

        return flags;
    }

}
