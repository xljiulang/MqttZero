using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace MqttZero.Protocol;

/// <summary>
/// 变长整数与无符号整数等线格式原语。
/// </summary>
internal static class MqttZeroWireFormat
{
    /// <summary>变长整数的最大字节数。</summary>
    public const int MaximumVariableByteIntegerBytes = 4;

    /// <summary>
    /// 尝试读取一个 MQTT 变长整数。
    /// </summary>
    /// <param name="reader">The source reader.</param>
    /// <param name="value">The decoded value when successful.</param>
    /// <returns><see langword="true"/> when one value was read.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadVariableByteInteger(
        ref SequenceReader<byte> reader,
        out int value)
    {
        value = 0;
        int multiplier = 1;
        for (int index = 0; index < MaximumVariableByteIntegerBytes; index++)
        {
            if (!reader.TryRead(out byte encoded))
            {
                value = 0;
                return false;
            }

            value += (encoded & 0x7F) * multiplier;
            if ((encoded & 0x80) == 0)
            {
                return true;
            }

            multiplier *= 128;
        }

        value = 0;
        return false;
    }

    /// <summary>
    /// 写入一个 MQTT 变长整数。
    /// </summary>
    /// <param name="destination">The destination span.</param>
    /// <param name="value">The non-negative value to write.</param>
    /// <returns>The number of bytes written.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int WriteVariableByteInteger(Span<byte> destination, int value)
    {
        int count = 0;
        do
        {
            byte encoded = (byte)(value % 128);
            value /= 128;
            if (value > 0)
            {
                encoded |= 0x80;
            }

            destination[count++] = encoded;
        }
        while (value > 0);

        return count;
    }

    /// <summary>
    /// 获取一个 MQTT 变长整数的编码长度。
    /// </summary>
    /// <param name="value">The value to measure.</param>
    /// <returns>The number of bytes needed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetVariableByteIntegerSize(long value)
    {
        if (value < 128)
        {
            return 1;
        }

        if (value < 16_384)
        {
            return 2;
        }

        if (value < 2_097_152)
        {
            return 3;
        }

        return 4;
    }

    /// <summary>
    /// 尝试读取一个大端 16 位无符号整数。
    /// </summary>
    /// <param name="reader">The source reader.</param>
    /// <param name="value">The decoded value when successful.</param>
    /// <returns><see langword="true"/> when two bytes were read.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadUInt16(ref SequenceReader<byte> reader, out ushort value)
    {
        value = 0;
        if (!reader.TryRead(out byte high) || !reader.TryRead(out byte low))
        {
            return false;
        }

        value = BinaryPrimitives.ReadUInt16BigEndian([high, low]);
        return true;
    }

    /// <summary>
    /// 在剩余字节足够时推进读取器。
    /// </summary>
    /// <param name="reader">The source reader.</param>
    /// <param name="length">The number of bytes to skip.</param>
    /// <returns><see langword="true"/> when the reader advanced.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryAdvance(ref SequenceReader<byte> reader, long length)
    {
        if (reader.Remaining < length)
        {
            return false;
        }

        reader.Advance(length);
        return true;
    }

    /// <summary>
    /// 跳过一段带长度前缀的二进制值。
    /// </summary>
    /// <param name="reader">The source reader.</param>
    /// <returns><see langword="true"/> when the value was skipped.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TrySkipBinary(ref SequenceReader<byte> reader)
    {
        if (!TryReadUInt16(ref reader, out ushort length) || reader.Remaining < length)
        {
            return false;
        }

        reader.Advance(length);
        return true;
    }
}
