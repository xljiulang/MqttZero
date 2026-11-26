using System.Buffers;
using System.Buffers.Binary;
using static MqttZero.Protocol.Mqtt5PropertyId;

namespace MqttZero.Protocol;

internal static class MqttPublishPacketCodec
{
    public static bool TryDecode(
        in ReadOnlySequence<byte> sequence,
        out MqttZeroPublishView view,
        out long consumed)
    {
        view = default;
        consumed = 0;
        if (sequence.IsEmpty)
        {
            return false;
        }

        SequenceReader<byte> reader = new(sequence);
        if (!reader.TryRead(out byte firstByte) || (firstByte >> 4) != (byte)MqttPacketType.Publish)
        {
            return false;
        }

        if (!TryReadVariableByteInteger(ref reader, out int remainingLength) ||
            reader.Remaining < remainingLength)
        {
            return false;
        }

        ReadOnlySequence<byte> body = sequence.Slice(reader.Position, remainingLength);
        SequenceReader<byte> bodyReader = new(body);
        if (!TryReadTopic(ref bodyReader, out ReadOnlySequence<byte> topic))
        {
            return false;
        }

        MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)((firstByte >> 1) & 0x03);
        if ((byte)qos > (byte)MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return false;
        }

        ushort packetIdentifier = 0;
        if (qos > MqttQualityOfServiceLevel.AtMostOnce &&
            (!TryReadUInt16(ref bodyReader, out packetIdentifier) ||
                packetIdentifier == 0))
        {
            return false;
        }

        if (!TryReadProperties(
            ref bodyReader,
            out ReadOnlySequence<byte> properties,
            out uint? messageExpiryInterval,
            out uint? subscriptionIdentifier,
            out ReadOnlyMemory<uint> subscriptionIdentifiers,
            out ushort? topicAlias))
        {
            return false;
        }

        ReadOnlySequence<byte> payload = bodyReader.Sequence.Slice(bodyReader.Position);
        view = new MqttZeroPublishView(
            topic,
            properties,
            payload,
            qos,
            packetIdentifier,
            (firstByte & 0x08) != 0,
            (firstByte & 0x01) != 0,
            messageExpiryInterval,
            subscriptionIdentifier,
            subscriptionIdentifiers,
            topicAlias);
        if ((!topic.IsEmpty && !MqttZeroProtocolValidator.IsValidTopicName(topic)) ||
            (topic.IsEmpty && !topicAlias.HasValue))
        {
            view = default;
            return false;
        }

        consumed = reader.Consumed + remainingLength;
        return true;
    }

    public static void Encode(in MqttZeroPublishView view, IBufferWriter<byte> writer)
    {
        int bodyLength = GetBodyLength(view);
        int propertyLength = GetPropertyLength(view);
        Span<byte> fixedHeader = writer.GetSpan(5);
        fixedHeader[0] = MqttPacketHeader.GetPublishFirstByte(
            view.Duplicate,
            view.QualityOfServiceLevel,
            view.Retain);
        int fixedHeaderLength = 1 + WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(fixedHeaderLength);
        WriteUInt16(writer, (ushort)view.Topic.Length);
        WriteSequence(writer, view.Topic);
        if (view.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce)
        {
            WriteUInt16(writer, view.PacketIdentifier);
        }

        Span<byte> propertyLengthSpan = writer.GetSpan(4);
        int propertyLengthBytes = WriteVariableByteInteger(propertyLengthSpan, propertyLength);
        writer.Advance(propertyLengthBytes);
        if (view.Properties.IsEmpty)
        {
            WriteScalarProperties(writer, view);
        }
        else
        {
            MqttZeroPublishPropertyTransformer.WriteWithoutMessageExpiry(
                writer,
                view.Properties,
                view.MessageExpiryInterval);
            WriteAdditionalScalarProperties(writer, view);
        }

        WriteSequence(writer, view.Payload);
    }

    public static MqttZeroPublishPacketBuffer EncodeHeader(
        in MqttZeroPublishView view,
        MqttZeroPacketWriter writer)
    {
        int propertyLength = GetPropertyLength(view);
        int bodyHeaderLength =
            2 +
            (int)view.Topic.Length +
            (view.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce ? 0 : 2) +
            GetVariableByteIntegerSize((uint)propertyLength) +
            propertyLength;
        uint remainingLength = (uint)bodyHeaderLength +
            (uint)view.Payload.Length;
        int remainingLengthSize = GetVariableByteIntegerSize(remainingLength);
        int headerLength = 1 + remainingLengthSize + bodyHeaderLength;
        writer.Reset();
        Span<byte> destination = writer.GetSpan(headerLength);
        int offset = 0;
        destination[offset++] = MqttPacketHeader.GetPublishFirstByte(
            view.Duplicate,
            view.QualityOfServiceLevel,
            view.Retain);
        offset += MqttZeroPacketWriter.WriteVariableByteInteger(
            destination[offset..],
            remainingLength);
        BinaryPrimitives.WriteUInt16BigEndian(
            destination[offset..],
            (ushort)view.Topic.Length);
        offset += 2;
        WriteSequence(destination, view.Topic, ref offset);
        if (view.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce)
        {
            BinaryPrimitives.WriteUInt16BigEndian(
                destination[offset..],
                view.PacketIdentifier);
            offset += 2;
        }

        offset += MqttZeroPacketWriter.WriteVariableByteInteger(
            destination[offset..],
            (uint)propertyLength);
        if (view.Properties.IsEmpty)
        {
            WriteScalarProperties(destination, view, ref offset);
        }
        else
        {
            offset += MqttZeroPublishPropertyTransformer.WriteWithoutMessageExpiry(
                destination[offset..],
                view.Properties,
                view.MessageExpiryInterval);
            WriteAdditionalScalarProperties(destination, view, ref offset);
        }

        writer.Advance(headerLength);
        return new MqttZeroPublishPacketBuffer(
            writer.WrittenMemory,
            view.Payload);
    }

    public static int EncodeToWriter(
        in MqttZeroPublishView view,
        MqttZeroPacketWriter headerWriter,
        IBufferWriter<byte> writer)
    {
        if (view.Payload.Length >= 64 * 1024 ||
            !view.Properties.IsEmpty)
        {
            int encodedLength = GetEncodedLength(view);
            Encode(view, writer);
            return encodedLength;
        }

        MqttZeroPublishPacketBuffer buffer = EncodeHeader(view, headerWriter);
        if (view.Payload.IsSingleSegment)
        {
            Span<byte> destination = writer.GetSpan(buffer.Length);
            buffer.Header.Span.CopyTo(destination);
            view.Payload.FirstSpan.CopyTo(destination[buffer.Header.Length..]);
            writer.Advance(buffer.Length);
            return buffer.Length;
        }

        Span<byte> header = writer.GetSpan(buffer.Header.Length);
        buffer.Header.Span.CopyTo(header);
        writer.Advance(buffer.Header.Length);
        WriteSequence(writer, view.Payload);
        return buffer.Length;
    }

    public static int GetEncodedLength(in MqttZeroPublishView view)
    {
        int bodyLength = GetBodyLength(view);
        return 1 + GetVariableByteIntegerSize((uint)bodyLength) + bodyLength;
    }

    private static bool TryReadProperties(
        ref SequenceReader<byte> reader,
        out ReadOnlySequence<byte> properties,
        out uint? messageExpiryInterval,
        out uint? subscriptionIdentifier,
        out ReadOnlyMemory<uint> subscriptionIdentifiers,
        out ushort? topicAlias)
    {
        properties = ReadOnlySequence<byte>.Empty;
        messageExpiryInterval = null;
        subscriptionIdentifier = null;
        subscriptionIdentifiers = ReadOnlyMemory<uint>.Empty;
        topicAlias = null;
        if (!TryReadVariableByteInteger(ref reader, out int length) ||
            reader.Remaining < length)
        {
            return false;
        }

        if (length == 0)
        {
            return true;
        }

        properties = reader.Sequence.Slice(reader.Position, length);
        SequenceReader<byte> propertyReader = new(properties);
        while (propertyReader.Remaining > 0)
        {
            if (!propertyReader.TryRead(out byte identifier))
            {
                return false;
            }

            switch (identifier)
            {
                case MessageExpiryInterval:
                    if (messageExpiryInterval.HasValue ||
                        !TryReadUInt32(ref propertyReader, out uint expiry))
                    {
                        return false;
                    }

                    messageExpiryInterval = expiry;
                    break;
                case SubscriptionIdentifier:
                    if (!TryReadVariableByteInteger(ref propertyReader, out int subscriptionId) ||
                        subscriptionId == 0)
                    {
                        return false;
                    }

                    if (!subscriptionIdentifier.HasValue &&
                        subscriptionIdentifiers.IsEmpty)
                    {
                        subscriptionIdentifier = (uint)subscriptionId;
                    }
                    else
                    {
                        uint first = subscriptionIdentifier ??
                            subscriptionIdentifiers.Span[0];
                        uint[] identifiers = subscriptionIdentifiers.IsEmpty
                            ? new[] { first, (uint)subscriptionId }
                            : new uint[subscriptionIdentifiers.Length + 1];
                        if (!subscriptionIdentifiers.IsEmpty)
                        {
                            subscriptionIdentifiers.Span.CopyTo(identifiers);
                            identifiers[^1] = (uint)subscriptionId;
                        }

                        subscriptionIdentifier = null;
                        subscriptionIdentifiers = identifiers;
                    }

                    break;
                case TopicAlias:
                    if (topicAlias.HasValue ||
                        !TryReadUInt16(ref propertyReader, out ushort alias) ||
                        alias == 0)
                    {
                        return false;
                    }

                    topicAlias = alias;
                    break;
                default:
                    if (!Mqtt5PropertySchema.IsAllowed(MqttPacketType.Publish, identifier) ||
                        !Mqtt5PropertyLayout.TrySkip(identifier, ref propertyReader))
                    {
                        return false;
                    }

                    break;
            }
        }

        reader.Advance(length);
        return true;
    }

    private static bool TryReadTopic(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> topic)
    {
        if (!TryReadUInt16(ref reader, out ushort length) || reader.Remaining < length)
        {
            topic = ReadOnlySequence<byte>.Empty;
            return false;
        }

        topic = length == 0
            ? ReadOnlySequence<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length);
        reader.Advance(length);
        return true;
    }

    private static bool TryReadUInt16(ref SequenceReader<byte> reader, out ushort value)
    {
        return MqttZeroWireFormat.TryReadUInt16(ref reader, out value);
    }

    private static bool TryReadUInt32(ref SequenceReader<byte> reader, out uint value)
    {
        value = 0;
        Span<byte> bytes = stackalloc byte[4];
        for (int index = 0; index < bytes.Length; index++)
        {
            if (!reader.TryRead(out bytes[index]))
            {
                return false;
            }
        }

        value = BinaryPrimitives.ReadUInt32BigEndian(bytes);
        return true;
    }

    private static bool TryReadVariableByteInteger(ref SequenceReader<byte> reader, out int value)
    {
        return MqttZeroWireFormat.TryReadVariableByteInteger(ref reader, out value);
    }

    private static bool TrySkipBinary(ref SequenceReader<byte> reader)
    {
        return MqttZeroWireFormat.TrySkipBinary(ref reader);
    }

    private static int GetScalarPropertyLength(in MqttZeroPublishView view)
    {
        int length = 0;
        if (view.MessageExpiryInterval.HasValue)
        {
            length += 5;
        }

        if (view.SubscriptionIdentifier.HasValue)
        {
            length += 1 + GetVariableByteIntegerSize(view.SubscriptionIdentifier.Value);
        }

        foreach (uint subscriptionIdentifier in view.SubscriptionIdentifiers.Span)
        {
            length += 1 + GetVariableByteIntegerSize(subscriptionIdentifier);
        }

        if (view.TopicAlias.HasValue)
        {
            length += 3;
        }

        return length;
    }

    private static int GetBodyLength(in MqttZeroPublishView view)
    {
        int propertyLength = GetPropertyLength(view);
        return
            2 +
            (int)view.Topic.Length +
            (view.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce ? 0 : 2) +
            GetVariableByteIntegerSize((uint)propertyLength) +
            propertyLength +
            (int)view.Payload.Length;
    }

    private static void WriteSequence(
        MqttZeroPacketWriter writer,
        ReadOnlySequence<byte> value)
    {
        if (value.IsSingleSegment)
        {
            writer.WriteBytes(value.FirstSpan);
            return;
        }

        foreach (ReadOnlyMemory<byte> segment in value)
        {
            writer.WriteBytes(segment.Span);
        }
    }

    private static void WriteScalarProperties(
        MqttZeroPacketWriter writer,
        in MqttZeroPublishView view)
    {
        if (view.MessageExpiryInterval.HasValue)
        {
            writer.WriteByte(MessageExpiryInterval);
            writer.WriteUInt32(view.MessageExpiryInterval.Value);
        }

        if (view.SubscriptionIdentifier.HasValue)
        {
            writer.WriteByte(SubscriptionIdentifier);
            writer.WriteVariableByteInteger(view.SubscriptionIdentifier.Value);
        }

        foreach (uint subscriptionIdentifier in view.SubscriptionIdentifiers.Span)
        {
            writer.WriteByte(SubscriptionIdentifier);
            writer.WriteVariableByteInteger(subscriptionIdentifier);
        }

        if (view.TopicAlias.HasValue)
        {
            writer.WriteByte(TopicAlias);
            writer.WriteUInt16(view.TopicAlias.Value);
        }
    }

    private static void WriteScalarProperties(
        Span<byte> destination,
        in MqttZeroPublishView view,
        ref int offset)
    {
        if (view.MessageExpiryInterval.HasValue)
        {
            destination[offset++] = MessageExpiryInterval;
            BinaryPrimitives.WriteUInt32BigEndian(
                destination[offset..],
                view.MessageExpiryInterval.Value);
            offset += sizeof(uint);
        }

        if (view.SubscriptionIdentifier.HasValue)
        {
            destination[offset++] = SubscriptionIdentifier;
            offset += MqttZeroPacketWriter.WriteVariableByteInteger(
                destination[offset..],
                view.SubscriptionIdentifier.Value);
        }

        foreach (uint subscriptionIdentifier in view.SubscriptionIdentifiers.Span)
        {
            destination[offset++] = SubscriptionIdentifier;
            offset += MqttZeroPacketWriter.WriteVariableByteInteger(
                destination[offset..],
                subscriptionIdentifier);
        }

        if (view.TopicAlias.HasValue)
        {
            destination[offset++] = TopicAlias;
            BinaryPrimitives.WriteUInt16BigEndian(
                destination[offset..],
                view.TopicAlias.Value);
            offset += sizeof(ushort);
        }
    }

    private static void WriteSequence(
        Span<byte> destination,
        ReadOnlySequence<byte> value,
        ref int offset)
    {
        value.CopyTo(destination[offset..]);
        offset += (int)value.Length;
    }

    private static int GetPropertyLength(in MqttZeroPublishView view)
    {
        if (view.Properties.IsEmpty)
        {
            return GetScalarPropertyLength(view);
        }

        int length = MqttZeroPublishPropertyTransformer.GetLengthWithoutMessageExpiry(
            view.Properties);
        if (view.MessageExpiryInterval.HasValue)
        {
            length += 5;
        }

        bool hasEncodedSubscriptionIdentifiers =
            MqttZeroPublishPropertyTransformer.ContainsProperty(
                view.Properties,
                SubscriptionIdentifier);
        if (view.SubscriptionIdentifier.HasValue &&
            !hasEncodedSubscriptionIdentifiers)
        {
            length += 1 + GetVariableByteIntegerSize(view.SubscriptionIdentifier.Value);
        }

        if (!hasEncodedSubscriptionIdentifiers)
        {
            foreach (uint subscriptionIdentifier in view.SubscriptionIdentifiers.Span)
            {
                length += 1 + GetVariableByteIntegerSize(subscriptionIdentifier);
            }
        }

        if (view.TopicAlias.HasValue &&
            !MqttZeroPublishPropertyTransformer.ContainsProperty(
                view.Properties,
                TopicAlias))
        {
            length += 3;
        }

        return length;
    }

    private static void WriteAdditionalScalarProperties(
        IBufferWriter<byte> writer,
        in MqttZeroPublishView view)
    {
        if (view.SubscriptionIdentifier.HasValue &&
            !MqttZeroPublishPropertyTransformer.ContainsProperty(
                view.Properties,
                SubscriptionIdentifier))
        {
            WriteByte(writer, SubscriptionIdentifier);
            WriteVariableByteInteger(writer, view.SubscriptionIdentifier.Value);
        }

        bool hasEncodedSubscriptionIdentifiers =
            MqttZeroPublishPropertyTransformer.ContainsProperty(
                view.Properties,
                SubscriptionIdentifier);
        if (!hasEncodedSubscriptionIdentifiers)
        {
            foreach (uint subscriptionIdentifier in view.SubscriptionIdentifiers.Span)
            {
                WriteByte(writer, SubscriptionIdentifier);
                WriteVariableByteInteger(writer, subscriptionIdentifier);
            }
        }

        if (view.TopicAlias.HasValue &&
            !MqttZeroPublishPropertyTransformer.ContainsProperty(
                view.Properties,
                TopicAlias))
        {
            WriteByte(writer, TopicAlias);
            WriteUInt16(writer, view.TopicAlias.Value);
        }
    }

    private static void WriteAdditionalScalarProperties(
        Span<byte> destination,
        in MqttZeroPublishView view,
        ref int offset)
    {
        if (view.SubscriptionIdentifier.HasValue &&
            !MqttZeroPublishPropertyTransformer.ContainsProperty(
                view.Properties,
                SubscriptionIdentifier))
        {
            destination[offset++] = SubscriptionIdentifier;
            offset += MqttZeroPacketWriter.WriteVariableByteInteger(
                destination[offset..],
                view.SubscriptionIdentifier.Value);
        }

        bool hasEncodedSubscriptionIdentifiers =
            MqttZeroPublishPropertyTransformer.ContainsProperty(
                view.Properties,
                SubscriptionIdentifier);
        if (!hasEncodedSubscriptionIdentifiers)
        {
            foreach (uint subscriptionIdentifier in view.SubscriptionIdentifiers.Span)
            {
                destination[offset++] = SubscriptionIdentifier;
                offset += MqttZeroPacketWriter.WriteVariableByteInteger(
                    destination[offset..],
                    subscriptionIdentifier);
            }
        }

        if (view.TopicAlias.HasValue &&
            !MqttZeroPublishPropertyTransformer.ContainsProperty(
                view.Properties,
                TopicAlias))
        {
            destination[offset++] = TopicAlias;
            BinaryPrimitives.WriteUInt16BigEndian(
                destination[offset..],
                view.TopicAlias.Value);
            offset += sizeof(ushort);
        }
    }

    private static void WriteScalarProperties(IBufferWriter<byte> writer, in MqttZeroPublishView view)
    {
        if (view.MessageExpiryInterval.HasValue)
        {
            WriteByte(writer, MessageExpiryInterval);
            WriteUInt32(writer, view.MessageExpiryInterval.Value);
        }

        if (view.SubscriptionIdentifier.HasValue)
        {
            WriteByte(writer, SubscriptionIdentifier);
            WriteVariableByteInteger(writer, view.SubscriptionIdentifier.Value);
        }

        foreach (uint subscriptionIdentifier in view.SubscriptionIdentifiers.Span)
        {
            WriteByte(writer, SubscriptionIdentifier);
            WriteVariableByteInteger(writer, subscriptionIdentifier);
        }

        if (view.TopicAlias.HasValue)
        {
            WriteByte(writer, TopicAlias);
            WriteUInt16(writer, view.TopicAlias.Value);
        }
    }

    private static void WriteSequence(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        foreach (ReadOnlyMemory<byte> segment in value)
        {
            Span<byte> destination = writer.GetSpan(segment.Length);
            segment.Span.CopyTo(destination);
            writer.Advance(segment.Length);
        }
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        Span<byte> destination = writer.GetSpan(1);
        destination[0] = value;
        writer.Advance(1);
    }

    private static void WriteUInt16(IBufferWriter<byte> writer, ushort value)
    {
        Span<byte> destination = writer.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(destination, value);
        writer.Advance(2);
    }

    private static void WriteUInt32(IBufferWriter<byte> writer, uint value)
    {
        Span<byte> destination = writer.GetSpan(4);
        BinaryPrimitives.WriteUInt32BigEndian(destination, value);
        writer.Advance(4);
    }

    private static int WriteVariableByteInteger(Span<byte> destination, int value)
    {
        return MqttZeroWireFormat.WriteVariableByteInteger(destination, value);
    }

    private static void WriteVariableByteInteger(IBufferWriter<byte> writer, uint value)
    {
        Span<byte> destination = writer.GetSpan(4);
        int length = WriteVariableByteInteger(destination, (int)value);
        writer.Advance(length);
    }

    private static int GetVariableByteIntegerSize(uint value)
    {
        return MqttZeroWireFormat.GetVariableByteIntegerSize(value);
    }
}
