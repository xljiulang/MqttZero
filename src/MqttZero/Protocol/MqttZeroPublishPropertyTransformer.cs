using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

internal static class MqttZeroPublishPropertyTransformer
{
    public static int GetLengthWithoutMessageExpiry(ReadOnlySequence<byte> properties)
    {
        if (properties.IsEmpty)
        {
            return 0;
        }

        int length = checked((int)properties.Length);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            properties.CopyTo(buffer);
            int filteredLength = 0;
            int offset = 0;
            while (offset < length)
            {
                if (!Mqtt5PropertyLayout.TryGetPropertyLength(
                        buffer.AsSpan(offset, length - offset),
                        out int propertyLength))
                {
                    return (int)properties.Length;
                }

                if (buffer[offset] != Mqtt5PropertyId.MessageExpiryInterval)
                {
                    filteredLength += propertyLength;
                }

                offset += propertyLength;
            }

            return filteredLength;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public static bool ContainsProperty(
        ReadOnlySequence<byte> properties,
        byte identifier)
    {
        if (properties.IsEmpty)
        {
            return false;
        }

        int length = checked((int)properties.Length);
        byte[] buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            properties.CopyTo(buffer);
            int offset = 0;
            while (offset < length)
            {
                if (!Mqtt5PropertyLayout.TryGetPropertyLength(
                        buffer.AsSpan(offset, length - offset),
                        out int propertyLength))
                {
                    return false;
                }

                if (buffer[offset] == identifier)
                {
                    return true;
                }

                offset += propertyLength;
            }

            return false;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
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
                if (!Mqtt5PropertyLayout.TryGetPropertyLength(
                        buffer.AsSpan(offset, length - offset),
                        out int propertyLength))
                {
                    throw new ArgumentException("The MQTT PUBLISH properties are invalid.");
                }

                if (buffer[offset] == Mqtt5PropertyId.MessageExpiryInterval)
                {
                    wroteMessageExpiry = true;
                    if (messageExpiryInterval.HasValue)
                    {
                        Span<byte> destination = writer.GetSpan(5);
                        destination[0] = Mqtt5PropertyId.MessageExpiryInterval;
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
                destination[0] = Mqtt5PropertyId.MessageExpiryInterval;
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
                if (!Mqtt5PropertyLayout.TryGetPropertyLength(
                        buffer.AsSpan(sourceOffset, length - sourceOffset),
                        out int propertyLength))
                {
                    throw new ArgumentException("The MQTT PUBLISH properties are invalid.");
                }

                if (buffer[sourceOffset] == Mqtt5PropertyId.MessageExpiryInterval)
                {
                    wroteMessageExpiry = true;
                    if (messageExpiryInterval.HasValue)
                    {
                        destination[destinationOffset++] =
                            Mqtt5PropertyId.MessageExpiryInterval;
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
                destination[destinationOffset++] =
                    Mqtt5PropertyId.MessageExpiryInterval;
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
}
