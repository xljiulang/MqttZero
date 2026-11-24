using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

internal static class MqttZeroPublishPropertyTransformer
{
    private const byte MessageExpiryIntervalId = 0x02;

    public static int GetLengthWithoutMessageExpiry(ReadOnlySequence<byte> properties)
    {
        if (properties.IsEmpty)
        {
            return 0;
        }

        if (properties.IsSingleSegment)
        {
            return GetLengthWithoutMessageExpiry(properties.FirstSpan);
        }

        SequenceReader<byte> reader = new(properties);
        int filteredLength = 0;
        while (reader.Remaining > 0)
        {
            long start = reader.Consumed;
            if (!TrySkipProperty(ref reader, out byte identifier))
            {
                return (int)properties.Length;
            }

            if (identifier != MessageExpiryIntervalId)
            {
                filteredLength += (int)(reader.Consumed - start);
            }
        }

        return filteredLength;
    }

    public static bool ContainsProperty(
        ReadOnlySequence<byte> properties,
        byte identifier)
    {
        if (properties.IsEmpty)
        {
            return false;
        }

        if (properties.IsSingleSegment)
        {
            return ContainsProperty(properties.FirstSpan, identifier);
        }

        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!TrySkipProperty(ref reader, out byte current))
            {
                return false;
            }

            if (current == identifier)
            {
                return true;
            }
        }

        return false;
    }

    public static void WriteWithoutMessageExpiry(
        IBufferWriter<byte> writer,
        ReadOnlySequence<byte> properties,
        uint? messageExpiryInterval)
    {
        int length = checked((int)properties.Length);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            properties.CopyTo(buffer);
            int offset = 0;
            bool wroteMessageExpiry = false;
            while (offset < length)
            {
                int propertyLength = GetPropertyLength(buffer.AsSpan(offset, length - offset));
                if (propertyLength <= 0)
                {
                    throw new ArgumentException("The MQTT PUBLISH properties are invalid.");
                }

                if (buffer[offset] == MessageExpiryIntervalId)
                {
                    wroteMessageExpiry = true;
                    if (messageExpiryInterval.HasValue)
                    {
                        Span<byte> destination = writer.GetSpan(5);
                        destination[0] = MessageExpiryIntervalId;
                        BinaryPrimitives.WriteUInt32BigEndian(
                            destination[1..],
                            messageExpiryInterval.Value);
                        writer.Advance(5);
                    }
                }
                else
                {
                    Span<byte> destination = writer.GetSpan(propertyLength);
                    buffer.AsSpan(offset, propertyLength).CopyTo(destination);
                    writer.Advance(propertyLength);
                }

                offset += propertyLength;
            }

            if (messageExpiryInterval.HasValue && !wroteMessageExpiry)
            {
                Span<byte> destination = writer.GetSpan(5);
                destination[0] = MessageExpiryIntervalId;
                BinaryPrimitives.WriteUInt32BigEndian(
                    destination[1..],
                    messageExpiryInterval.Value);
                writer.Advance(5);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static int WriteWithoutMessageExpiry(
        Span<byte> destination,
        ReadOnlySequence<byte> properties,
        uint? messageExpiryInterval)
    {
        int length = checked((int)properties.Length);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            properties.CopyTo(buffer);
            int sourceOffset = 0;
            int destinationOffset = 0;
            bool wroteMessageExpiry = false;
            while (sourceOffset < length)
            {
                int propertyLength = GetPropertyLength(buffer.AsSpan(sourceOffset, length - sourceOffset));
                if (propertyLength <= 0)
                {
                    throw new ArgumentException("The MQTT PUBLISH properties are invalid.");
                }

                if (buffer[sourceOffset] == MessageExpiryIntervalId)
                {
                    wroteMessageExpiry = true;
                    if (messageExpiryInterval.HasValue)
                    {
                        destination[destinationOffset++] = MessageExpiryIntervalId;
                        BinaryPrimitives.WriteUInt32BigEndian(
                            destination[destinationOffset..],
                            messageExpiryInterval.Value);
                        destinationOffset += sizeof(uint);
                    }
                }
                else
                {
                    buffer.AsSpan(sourceOffset, propertyLength)
                        .CopyTo(destination[destinationOffset..]);
                    destinationOffset += propertyLength;
                }

                sourceOffset += propertyLength;
            }

            if (messageExpiryInterval.HasValue && !wroteMessageExpiry)
            {
                destination[destinationOffset++] = MessageExpiryIntervalId;
                BinaryPrimitives.WriteUInt32BigEndian(
                    destination[destinationOffset..],
                    messageExpiryInterval.Value);
                destinationOffset += sizeof(uint);
            }

            return destinationOffset;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static int GetLengthWithoutMessageExpiry(
        ReadOnlySpan<byte> properties)
    {
        int filteredLength = 0;
        int offset = 0;
        while (offset < properties.Length)
        {
            int propertyLength = GetPropertyLength(properties[offset..]);
            if (propertyLength <= 0)
            {
                return properties.Length;
            }

            if (properties[offset] != MessageExpiryIntervalId)
            {
                filteredLength += propertyLength;
            }

            offset += propertyLength;
        }

        return filteredLength;
    }

    private static bool ContainsProperty(
        ReadOnlySpan<byte> properties,
        byte identifier)
    {
        int offset = 0;
        while (offset < properties.Length)
        {
            int propertyLength = GetPropertyLength(properties[offset..]);
            if (propertyLength <= 0)
            {
                return false;
            }

            if (properties[offset] == identifier)
            {
                return true;
            }

            offset += propertyLength;
        }

        return false;
    }

    private static int GetPropertyLength(ReadOnlySpan<byte> properties)
    {
        if (properties.IsEmpty)
        {
            return -1;
        }

        byte identifier = properties[0];
        if (identifier == 0x0B)
        {
            for (int index = 1; index < Math.Min(properties.Length, 5); index++)
            {
                if ((properties[index] & 0x80) == 0)
                {
                    return index + 1;
                }
            }

            return -1;
        }

        int fixedLength = identifier switch
        {
            0x01 or 0x17 or 0x19 or 0x24 or 0x25 or 0x28 or 0x29 or 0x2A => 2,
            0x11 or 0x27 or MessageExpiryIntervalId => 5,
            0x13 or 0x21 or 0x22 or 0x23 => 3,
            _ => 0
        };

        if (fixedLength > 0)
        {
            return properties.Length >= fixedLength ? fixedLength : -1;
        }

        if (identifier == 0x26)
        {
            int firstEnd = TryReadBinaryLength(properties, 1);
            if (firstEnd < 0)
            {
                return -1;
            }

            return TryReadBinaryLength(properties, firstEnd);
        }

        return identifier is
            0x09 or 0x16 or 0x03 or 0x08 or 0x12 or 0x15 or 0x18 or 0x1A or 0x1C or 0x1F
                ? TryReadBinaryLength(properties, 1)
                : -1;
    }

    private static bool TrySkipProperty(
        ref SequenceReader<byte> reader,
        out byte identifier)
    {
        identifier = 0;
        if (!reader.TryRead(out identifier))
        {
            return false;
        }

        if (identifier == 0x0B)
        {
            for (int index = 0; index < 4; index++)
            {
                if (!reader.TryRead(out byte encoded))
                {
                    return false;
                }

                if ((encoded & 0x80) == 0)
                {
                    return true;
                }
            }

            return false;
        }

        int fixedPayloadLength = identifier switch
        {
            0x01 or 0x17 or 0x19 or 0x24 or 0x25 or 0x28 or 0x29 or 0x2A => 1,
            0x11 or 0x27 or MessageExpiryIntervalId => 4,
            0x13 or 0x21 or 0x22 or 0x23 => 2,
            _ => 0
        };
        if (fixedPayloadLength > 0)
        {
            return TryAdvance(ref reader, fixedPayloadLength);
        }

        if (identifier == 0x26)
        {
            return TrySkipBinary(ref reader) &&
                TrySkipBinary(ref reader);
        }

        return identifier is
            0x09 or 0x16 or 0x03 or 0x08 or 0x12 or 0x15 or 0x18 or 0x1A or 0x1C or 0x1F
                ? TrySkipBinary(ref reader)
                : false;
    }

    private static bool TrySkipBinary(ref SequenceReader<byte> reader)
    {
        if (!reader.TryRead(out byte high) ||
            !reader.TryRead(out byte low))
        {
            return false;
        }

        int length = (high << 8) | low;
        if (reader.Remaining < length)
        {
            return false;
        }

        reader.Advance(length);
        return true;
    }

    private static bool TryAdvance(
        ref SequenceReader<byte> reader,
        long length)
    {
        if (reader.Remaining < length)
        {
            return false;
        }

        reader.Advance(length);
        return true;
    }

    private static int TryReadBinaryLength(
        ReadOnlySpan<byte> properties,
        int offset)
    {
        if (offset + 2 > properties.Length)
        {
            return -1;
        }

        int payloadLength = (properties[offset] << 8) | properties[offset + 1];
        int endOffset = offset + 2 + payloadLength;
        return endOffset <= properties.Length ? endOffset : -1;
    }
}
