using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// Provides shared MQTT packet codec read and write helpers.
/// </summary>
public static partial class MqttPacketCodec
{
    private static bool TryReadProperties(
        ref SequenceReader<byte> reader,
        out ReadOnlySequence<byte> properties)
    {
        properties = ReadOnlySequence<byte>.Empty;
        if (!MqttZeroWireFormat.TryReadVariableByteInteger(ref reader, out int propertyLength) ||
            propertyLength < 0 ||
            reader.Remaining < propertyLength)
        {
            return false;
        }

        if (propertyLength > 0)
        {
            properties = reader.Sequence.Slice(reader.Position, propertyLength);
        }

        reader.Advance(propertyLength);
        return true;
    }

    private static bool TryReadUtf8(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> value)
    {
        return TryReadBinary(ref reader, out value) &&
            MqttZeroProtocolValidator.IsValidUtf8(value);
    }

    private static bool TryReadBinary(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> value)
    {
        if (!MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort length) || reader.Remaining < length)
        {
            value = ReadOnlySequence<byte>.Empty;
            return false;
        }

        value = length == 0
            ? ReadOnlySequence<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length);
        reader.Advance(length);
        return true;
    }

    private static void WriteProperties(IBufferWriter<byte> writer, ReadOnlySequence<byte> properties)
    {
        int length = (int)properties.Length;
        Span<byte> lengthSpan = writer.GetSpan(4);
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(lengthSpan, length);
        writer.Advance(lengthBytes);
        WriteSequence(writer, properties);
    }

    private static void WriteUtf8(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT UTF-8 values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        Span<byte> destination = writer.GetSpan(value.Length);
        value.CopyTo(destination);
        writer.Advance(value.Length);
    }

    private static void WriteUtf8(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT UTF-8 values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        WriteSequence(writer, value);
    }

    private static void WriteBinary(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT binary values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        WriteSequence(writer, value);
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
}
