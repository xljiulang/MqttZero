using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 报文的统一解码与编码入口，按报文族拆成多个分部文件。
/// </summary>
internal static partial class MqttPacketCodec
{
    /// <summary>
    /// 尝试从序列中解码一个 MQTT 报文。
    /// </summary>
    /// <param name="sequence">The input byte sequence.</param>
    /// <param name="packet">The decoded packet when successful.</param>
    /// <param name="consumed">The number of bytes consumed by the packet.</param>
    /// <returns><see langword="true"/> when a complete supported packet was decoded.</returns>
    public static bool TryDecode(
        in ReadOnlySequence<byte> sequence,
        out MqttPacket? packet,
        out long consumed)
    {
        packet = null;
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

        if (!MqttZeroWireFormat.TryReadVariableByteInteger(ref reader, out int remainingLength) || remainingLength < 0)
        {
            return false;
        }

        if (reader.Remaining < remainingLength)
        {
            return false;
        }

        ReadOnlySequence<byte> body = sequence.Slice(reader.Position, remainingLength);
        SequenceReader<byte> bodyReader = new(body);
        MqttPacketType packetType = (MqttPacketType)(firstByte >> 4);
        bool success = packetType switch
        {
            MqttPacketType.Connect => TryDecodeConnect(firstByte, ref bodyReader, out packet),
            MqttPacketType.ConnAck => TryDecodeConnAck(firstByte, ref bodyReader, out packet),
            MqttPacketType.Publish => TryDecodePublish(firstByte, ref bodyReader, out packet),
            MqttPacketType.PubAck => TryDecodePubAck(firstByte, ref bodyReader, out packet),
            MqttPacketType.PubRec => TryDecodePubRec(firstByte, ref bodyReader, out packet),
            MqttPacketType.PubRel => TryDecodePubRel(firstByte, ref bodyReader, out packet),
            MqttPacketType.PubComp => TryDecodePubComp(firstByte, ref bodyReader, out packet),
            MqttPacketType.Subscribe => TryDecodeSubscribe(firstByte, ref bodyReader, out packet),
            MqttPacketType.SubAck => TryDecodeSubAck(firstByte, ref bodyReader, out packet),
            MqttPacketType.Unsubscribe => TryDecodeUnsubscribe(firstByte, ref bodyReader, out packet),
            MqttPacketType.UnsubAck => TryDecodeUnsubAck(firstByte, ref bodyReader, out packet),
            MqttPacketType.PingReq => TryDecodePingReq(firstByte, ref bodyReader, out packet),
            MqttPacketType.Disconnect => TryDecodeDisconnect(firstByte, ref bodyReader, out packet),
            _ => false
        };

        if (!success)
        {
            packet = null;
            return false;
        }

        consumed = reader.Consumed + remainingLength;
        return true;
    }

    /// <summary>
    /// 尝试计算序列中首个 MQTT 报文的编码总长度。
    /// </summary>
    /// <param name="sequence">The input byte sequence.</param>
    /// <param name="packetLength">The total packet length when the fixed header is available.</param>
    /// <param name="complete">Whether the entire packet is present.</param>
    /// <returns><see langword="true"/> when the variable-length header could be read.</returns>
    public static bool TryGetPacketLength(
        in ReadOnlySequence<byte> sequence,
        out long packetLength,
        out bool complete)
    {
        packetLength = 0;
        complete = false;
        if (sequence.IsEmpty)
        {
            return false;
        }

        SequenceReader<byte> reader = new(sequence);
        _ = reader.TryRead(out _);
        if (!MqttZeroWireFormat.TryReadVariableByteInteger(ref reader, out int remainingLength))
        {
            return false;
        }

        packetLength = reader.Consumed + remainingLength;
        complete = reader.Remaining >= remainingLength;
        return true;
    }

    /// <summary>
    /// 把一个 MQTT 报文编码到缓冲写入器。
    /// </summary>
    /// <param name="packet">The packet to encode.</param>
    /// <param name="writer">The destination buffer writer.</param>
    /// <exception cref="ArgumentNullException">Thrown when arguments are null.</exception>
    /// <exception cref="NotSupportedException">Thrown for unsupported packet types.</exception>
    public static void Encode(MqttPacket packet, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(packet);
        ArgumentNullException.ThrowIfNull(writer);

        switch (packet)
        {
            case MqttConnectPacket connectPacket:
                EncodeConnect(connectPacket, writer);
                break;
            case MqttConnAckPacket connAckPacket:
                EncodeConnAck(connAckPacket, writer);
                break;
            case MqttPublishPacket publishPacket:
                EncodePublish(publishPacket, writer);
                break;
            case MqttPubAckPacket pubAckPacket:
                EncodePubAck(pubAckPacket, writer);
                break;
            case MqttPubRecPacket pubRecPacket:
                EncodeAcknowledgement(
                    MqttPacketHeader.GetFirstByte(MqttPacketType.PubRec),
                    pubRecPacket.PacketIdentifier,
                    pubRecPacket.ReasonCode,
                    pubRecPacket.ReasonString,
                    pubRecPacket.UserProperties,
                    writer);
                break;
            case MqttPubRelPacket pubRelPacket:
                EncodeAcknowledgement(
                    MqttPacketHeader.GetFirstByte(MqttPacketType.PubRel),
                    pubRelPacket.PacketIdentifier,
                    pubRelPacket.ReasonCode,
                    pubRelPacket.ReasonString,
                    pubRelPacket.UserProperties,
                    writer);
                break;
            case MqttPubCompPacket pubCompPacket:
                EncodeAcknowledgement(
                    MqttPacketHeader.GetFirstByte(MqttPacketType.PubComp),
                    pubCompPacket.PacketIdentifier,
                    pubCompPacket.ReasonCode,
                    pubCompPacket.ReasonString,
                    pubCompPacket.UserProperties,
                    writer);
                break;
            case MqttSubscribePacket subscribePacket:
                EncodeSubscribe(subscribePacket, writer);
                break;
            case MqttSubAckPacket subAckPacket:
                EncodeSubAck(subAckPacket, writer);
                break;
            case MqttUnsubscribePacket unsubscribePacket:
                EncodeUnsubscribe(unsubscribePacket, writer);
                break;
            case MqttUnsubAckPacket unsubAckPacket:
                EncodeUnsubAck(unsubAckPacket, writer);
                break;
            case MqttPingReqPacket:
                EncodeEmptyPacket(MqttPacketType.PingReq, writer);
                break;
            case MqttPingRespPacket:
                EncodeEmptyPacket(MqttPacketType.PingResp, writer);
                break;
            case MqttDisconnectPacket disconnectPacket:
                EncodeDisconnect(disconnectPacket, writer);
                break;
            default:
                throw new NotSupportedException($"Packet type '{packet.PacketType}' is not supported.");
        }
    }

}
