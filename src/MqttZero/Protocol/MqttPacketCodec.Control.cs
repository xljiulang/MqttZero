using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// Implements MQTT acknowledgement and control packet codecs.
/// </summary>
public static partial class MqttPacketCodec
{
    internal static bool TryDecodeAcknowledgement(
        in ReadOnlySequence<byte> sequence,
        out MqttZeroAcknowledgementView view,
        out long consumed)
    {
        view = default;
        consumed = 0;
        if (sequence.IsEmpty)
        {
            return false;
        }

        SequenceReader<byte> reader = new(sequence);
        if (!reader.TryRead(out byte firstByte))
        {
            return false;
        }

        MqttPacketType packetType = (MqttPacketType)(firstByte >> 4);
        if (!IsAcknowledgementPacketType(packetType) ||
            !TryReadVariableByteInteger(ref reader, out int remainingLength) ||
            remainingLength < 0 ||
            reader.Remaining < remainingLength)
        {
            return false;
        }

        ReadOnlySequence<byte> body = sequence.Slice(reader.Position, remainingLength);
        SequenceReader<byte> bodyReader = new(body);
        if (!TryReadUInt16(ref bodyReader, out ushort packetIdentifier) ||
            packetIdentifier == 0)
        {
            return false;
        }

        MqttReasonCode reasonCode = MqttReasonCode.Success;
        if (bodyReader.Remaining > 0)
        {
            if (!bodyReader.TryRead(out byte encodedReasonCode))
            {
                return false;
            }

            reasonCode = (MqttReasonCode)encodedReasonCode;
            if (bodyReader.Remaining > 0)
            {
                if (!TryReadProperties(
                        ref bodyReader,
                        out ReadOnlySequence<byte> properties) ||
                    !Mqtt5PropertyReader.TryValidateReasonProperties(properties))
                {
                    return false;
                }
            }
        }

        if (bodyReader.Remaining != 0 ||
            (packetType == MqttPacketType.PubRel &&
                (firstByte & 0x0F) != 0x02) ||
            (packetType != MqttPacketType.PubRel &&
                (firstByte & 0x0F) != 0) ||
            !IsValidAcknowledgementReasonCode(packetType, reasonCode))
        {
            return false;
        }

        consumed = reader.Consumed + remainingLength;
        view = new MqttZeroAcknowledgementView(
            packetType,
            packetIdentifier,
            reasonCode);
        return true;
    }

    internal static bool IsAcknowledgementPacketType(MqttPacketType packetType)
    {
        return packetType is
            MqttPacketType.PubAck or
            MqttPacketType.PubRec or
            MqttPacketType.PubRel or
            MqttPacketType.PubComp;
    }

    private static bool IsValidAcknowledgementReasonCode(
        MqttPacketType packetType,
        MqttReasonCode reasonCode)
    {
        return packetType switch
        {
            MqttPacketType.PubAck or MqttPacketType.PubRec =>
                MqttZeroProtocolValidator.IsValidPubAckReasonCode(reasonCode),
            MqttPacketType.PubRel or MqttPacketType.PubComp =>
                MqttZeroProtocolValidator.IsValidPubRelReasonCode(reasonCode),
            _ => false
        };
    }

    private static bool TryDecodeConnAck(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !reader.TryRead(out byte acknowledgeFlags) ||
            (acknowledgeFlags & 0xFE) != 0 ||
            !reader.TryRead(out byte reasonCode) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties) ||
            reader.Remaining != 0)
        {
            return false;
        }

        MqttReasonCode connAckReason = (MqttReasonCode)reasonCode;
        if (!MqttZeroProtocolValidator.IsValidConnAckReasonCode(connAckReason) ||
            (connAckReason != MqttReasonCode.Success && (acknowledgeFlags & 0x01) != 0))
        {
            return false;
        }

        packet = new MqttConnAckPacket
        {
            SessionPresent = (acknowledgeFlags & 0x01) != 0,
            ReasonCode = connAckReason
        };
        if (!Mqtt5PropertyReader.TryReadConnAck(properties, (MqttConnAckPacket)packet))
        {
            packet = null;
            return false;
        }

        return true;
    }

    private static bool TryDecodePubAck(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadAcknowledgement(
                ref reader,
                out ushort packetIdentifier,
                out MqttReasonCode reasonCode,
                out string? reasonString,
                out IReadOnlyList<MqttUserProperty> userProperties))
        {
            return false;
        }

        packet = new MqttPubAckPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            ReasonString = reasonString,
            UserProperties = userProperties
        };
        return reader.Remaining == 0 &&
            MqttZeroProtocolValidator.IsValidPubAckReasonCode(reasonCode);
    }

    private static bool TryDecodePubRec(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadAcknowledgement(
                ref reader,
                out ushort packetIdentifier,
                out MqttReasonCode reasonCode,
                out string? reasonString,
                out IReadOnlyList<MqttUserProperty> userProperties))
        {
            return false;
        }

        packet = new MqttPubRecPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            ReasonString = reasonString,
            UserProperties = userProperties
        };
        return MqttZeroProtocolValidator.IsValidPubAckReasonCode(reasonCode);
    }

    private static bool TryDecodePubRel(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0x02 ||
            !TryReadAcknowledgement(
                ref reader,
                out ushort packetIdentifier,
                out MqttReasonCode reasonCode,
                out string? reasonString,
                out IReadOnlyList<MqttUserProperty> userProperties))
        {
            return false;
        }

        packet = new MqttPubRelPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            ReasonString = reasonString,
            UserProperties = userProperties
        };
        return MqttZeroProtocolValidator.IsValidPubRelReasonCode(reasonCode);
    }

    private static bool TryDecodePubComp(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadAcknowledgement(
                ref reader,
                out ushort packetIdentifier,
                out MqttReasonCode reasonCode,
                out string? reasonString,
                out IReadOnlyList<MqttUserProperty> userProperties))
        {
            return false;
        }

        packet = new MqttPubCompPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            ReasonString = reasonString,
            UserProperties = userProperties
        };
        return MqttZeroProtocolValidator.IsValidPubRelReasonCode(reasonCode);
    }

    private static bool TryReadAcknowledgement(
        ref SequenceReader<byte> reader,
        out ushort packetIdentifier,
        out MqttReasonCode reasonCode,
        out string? reasonString,
        out IReadOnlyList<MqttUserProperty> userProperties)
    {
        reasonCode = MqttReasonCode.Success;
        reasonString = null;
        userProperties = Array.Empty<MqttUserProperty>();
        if (!TryReadUInt16(ref reader, out packetIdentifier))
        {
            return false;
        }

        if (packetIdentifier == 0)
        {
            return false;
        }

        if (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte encodedReasonCode))
            {
                return false;
            }

            reasonCode = (MqttReasonCode)encodedReasonCode;
            if (reader.Remaining > 0)
            {
                if (!TryReadProperties(ref reader, out ReadOnlySequence<byte> properties) ||
                    !Mqtt5PropertyReader.TryReadReasonProperties(
                        properties,
                        out reasonString,
                        out userProperties))
                {
                    return false;
                }
            }
        }

        return reader.Remaining == 0;
    }

    private static void EncodeAcknowledgement(
        byte firstByte,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        string? reasonString,
        IReadOnlyList<MqttUserProperty> userProperties,
        IBufferWriter<byte> writer)
    {
        if (packetIdentifier == 0)
        {
            throw new ArgumentException("The MQTT acknowledgement packet identifier is invalid.");
        }

        if ((firstByte == 0x50 &&
                !MqttZeroProtocolValidator.IsValidPubAckReasonCode(reasonCode)) ||
            ((firstByte == 0x62 || firstByte == 0x70) &&
                !MqttZeroProtocolValidator.IsValidPubRelReasonCode(reasonCode)))
        {
            throw new ArgumentException("The MQTT acknowledgement reason code is invalid.");
        }

        int propertiesLength = Mqtt5PropertyWriter.GetReasonPropertiesLength(
            reasonString,
            userProperties);
        bool includeReason = reasonCode != MqttReasonCode.Success || propertiesLength > 0;
        int bodyLength = includeReason
            ? 3 +
                GetVariableByteIntegerSize(propertiesLength) +
                propertiesLength
            : 2;
        Span<byte> fixedHeader = writer.GetSpan(5);
        fixedHeader[0] = firstByte;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packetIdentifier);
        if (includeReason)
        {
            WriteByte(writer, (byte)reasonCode);
            Mqtt5PropertyWriter.WriteReasonProperties(
                writer,
                reasonString,
                userProperties);
        }
    }

    private static bool TryDecodePingReq(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0)
        {
            return false;
        }

        packet = new MqttPingReqPacket();
        return reader.Remaining == 0;
    }

    private static bool TryDecodeDisconnect(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0)
        {
            return false;
        }

        MqttReasonCode reasonCode = MqttReasonCode.Success;
        ReadOnlySequence<byte> properties = ReadOnlySequence<byte>.Empty;
        if (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte encodedReasonCode))
            {
                return false;
            }

            reasonCode = (MqttReasonCode)encodedReasonCode;
            if (!TryReadProperties(ref reader, out properties))
            {
                return false;
            }
        }

        packet = new MqttDisconnectPacket
        {
            ReasonCode = reasonCode
        };
        if (!MqttZeroProtocolValidator.IsValidDisconnectReasonCode(reasonCode) ||
            !Mqtt5PropertyReader.TryReadDisconnect(
                properties,
                (MqttDisconnectPacket)packet))
        {
            packet = null;
            return false;
        }

        return reader.Remaining == 0;
    }

    private static void EncodePubAck(MqttPubAckPacket packet, IBufferWriter<byte> writer)
    {
        if (packet.PacketIdentifier == 0 ||
            !MqttZeroProtocolValidator.IsValidPubAckReasonCode(packet.ReasonCode))
        {
            throw new ArgumentException("The MQTT PUBACK packet is invalid.", nameof(packet));
        }

        int propertiesLength = Mqtt5PropertyWriter.GetReasonPropertiesLength(
            packet.ReasonString,
            packet.UserProperties);
        bool includeReason = packet.ReasonCode != MqttReasonCode.Success ||
            propertiesLength > 0;
        int bodyLength = includeReason
            ? 3 + GetVariableByteIntegerSize(propertiesLength) + propertiesLength
            : 2;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0x40;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);

        if (includeReason)
        {
            WriteByte(writer, (byte)packet.ReasonCode);
            Mqtt5PropertyWriter.WriteReasonProperties(
                writer,
                packet.ReasonString,
                packet.UserProperties);
        }
    }

    private static void EncodeEmptyPacket(MqttPacketType packetType, IBufferWriter<byte> writer)
    {
        Span<byte> destination = writer.GetSpan(2);
        destination[0] = (byte)((byte)packetType << 4);
        destination[1] = 0;
        writer.Advance(2);
    }

    private static void EncodeDisconnect(MqttDisconnectPacket packet, IBufferWriter<byte> writer)
    {
        if (!MqttZeroProtocolValidator.IsValidDisconnectReasonCode(packet.ReasonCode))
        {
            throw new ArgumentException("The MQTT DISCONNECT reason code is invalid.");
        }

        int propertiesLength = Mqtt5PropertyWriter.GetDisconnectPropertiesLength(packet);
        bool includeDetail =
            packet.ReasonCode != MqttReasonCode.Success ||
            propertiesLength > 0;
        int bodyLength = includeDetail
            ? 1 + GetVariableByteIntegerSize(propertiesLength) + propertiesLength
            : 0;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0xE0;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);

        if (includeDetail)
        {
            WriteByte(writer, (byte)packet.ReasonCode);
            Mqtt5PropertyWriter.WriteDisconnectProperties(packet, writer);
        }
    }

}
