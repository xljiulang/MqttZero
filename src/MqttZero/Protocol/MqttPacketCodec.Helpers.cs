using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// MQTT 报文的统一解码与编码入口，按报文族拆成多个分部文件。
/// </summary>
public static partial class MqttPacketCodec
{
    /// <summary>
    /// 尝试读取属性。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <param name="properties">属性。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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

    /// <summary>
    /// 尝试读取UTF-8。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool TryReadUtf8(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> value)
    {
        return TryReadBinary(ref reader, out value) &&
            MqttZeroProtocolValidator.IsValidUtf8(value);
    }

    /// <summary>
    /// 尝试读取二进制。
    /// </summary>
    /// <param name="reader">读取器。</param>
    /// <param name="value">值。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
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

    /// <summary>
    /// 写入属性。
    /// </summary>
    /// <param name="writer">写入器。</param>
    /// <param name="properties">属性。</param>
    private static void WriteProperties(IBufferWriter<byte> writer, ReadOnlySequence<byte> properties)
    {
        int length = (int)properties.Length;
        Span<byte> lengthSpan = writer.GetSpan(4);
        int lengthBytes = MqttZeroWireFormat.WriteVariableByteInteger(lengthSpan, length);
        writer.Advance(lengthBytes);
        WriteSequence(writer, properties);
    }

    /// <summary>
    /// 写入UTF-8。
    /// </summary>
    /// <param name="writer">写入器。</param>
    /// <param name="value">值。</param>
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

    /// <summary>
    /// 写入UTF-8。
    /// </summary>
    /// <param name="writer">写入器。</param>
    /// <param name="value">值。</param>
    private static void WriteUtf8(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT UTF-8 values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        WriteSequence(writer, value);
    }

    /// <summary>
    /// 写入二进制。
    /// </summary>
    /// <param name="writer">写入器。</param>
    /// <param name="value">值。</param>
    private static void WriteBinary(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT binary values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        WriteSequence(writer, value);
    }

    /// <summary>
    /// 写入序列。
    /// </summary>
    /// <param name="writer">写入器。</param>
    /// <param name="value">值。</param>
    private static void WriteSequence(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        foreach (ReadOnlyMemory<byte> segment in value)
        {
            Span<byte> destination = writer.GetSpan(segment.Length);
            segment.Span.CopyTo(destination);
            writer.Advance(segment.Length);
        }
    }

    /// <summary>
    /// 写入字节。
    /// </summary>
    /// <param name="writer">写入器。</param>
    /// <param name="value">值。</param>
    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        Span<byte> destination = writer.GetSpan(1);
        destination[0] = value;
        writer.Advance(1);
    }

    /// <summary>
    /// 写入UInt16。
    /// </summary>
    /// <param name="writer">写入器。</param>
    /// <param name="value">值。</param>
    private static void WriteUInt16(IBufferWriter<byte> writer, ushort value)
    {
        Span<byte> destination = writer.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(destination, value);
        writer.Advance(2);
    }
}
