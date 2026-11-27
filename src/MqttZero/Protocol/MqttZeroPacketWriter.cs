using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// 可扩容的分段报文写入器。
/// </summary>
internal sealed class MqttZeroPacketWriter : IBufferWriter<byte>
{
    private byte[] _buffer = new byte[256];
    private int _length;
    private int _position;

    public int WrittenCount
    {
        get
        {
            return _length;
        }
    }

    public ReadOnlyMemory<byte> WrittenMemory
    {
        get
        {
            return _buffer.AsMemory(0, _length);
        }
    }

    /// <summary>
    /// 重置。
    /// </summary>
    /// <param name="reservedLength">预留长度。</param>
    public void Reset(int reservedLength = 0)
    {
        _position = reservedLength;
        _length = reservedLength;
    }

    /// <summary>
    /// 定位。
    /// </summary>
    /// <param name="position">位置。</param>
    public void Seek(int position)
    {
        EnsureCapacity(position);
        _position = position;
    }

    /// <summary>
    /// 推进。
    /// </summary>
    /// <param name="count">数量。</param>
    public void Advance(int count)
    {
        _position += count;
        if (_position > _length)
        {
            _length = _position;
        }
    }

    /// <summary>
    /// 清空。
    /// </summary>
    public void Clear()
    {
        Reset();
    }

    /// <summary>
    /// 获取内存。
    /// </summary>
    /// <param name="sizeHint">大小提示。</param>
    /// <returns>可直接写入的字节内存块。</returns>
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(_position + Math.Max(sizeHint, 1));
        return _buffer.AsMemory(_position);
    }

    /// <summary>
    /// 获取跨度。
    /// </summary>
    /// <param name="sizeHint">大小提示。</param>
    /// <returns>可直接写入的字节内存块。</returns>
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(_position + Math.Max(sizeHint, 1));
        return _buffer.AsSpan(_position);
    }

    /// <summary>
    /// 写入字节。
    /// </summary>
    /// <param name="value">值。</param>
    public void WriteByte(byte value)
    {
        EnsureCapacity(_position + 1);
        _buffer[_position++] = value;
        _length = Math.Max(_length, _position);
    }

    /// <summary>
    /// 写入UInt16。
    /// </summary>
    /// <param name="value">值。</param>
    public void WriteUInt16(ushort value)
    {
        Span<byte> span = GetSpan(sizeof(ushort));
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        Advance(sizeof(ushort));
    }

    /// <summary>
    /// 写入UInt32。
    /// </summary>
    /// <param name="value">值。</param>
    public void WriteUInt32(uint value)
    {
        Span<byte> span = GetSpan(sizeof(uint));
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        Advance(sizeof(uint));
    }

    /// <summary>
    /// 写入变长字节整数。
    /// </summary>
    /// <param name="value">值。</param>
    public void WriteVariableByteInteger(uint value)
    {
        Span<byte> destination = GetSpan(4);
        int count = MqttZeroWireFormat.WriteVariableByteInteger(
            destination,
            (int)value);
        Advance(count);
    }

    /// <summary>
    /// 写入字节数。
    /// </summary>
    /// <param name="value">值。</param>
    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        Span<byte> destination = GetSpan(value.Length);
        value.CopyTo(destination);
        Advance(value.Length);
    }

    /// <summary>
    /// 确保容量。
    /// </summary>
    /// <param name="required">必需。</param>
    private void EnsureCapacity(int required)
    {
        if (_buffer.Length >= required)
        {
            return;
        }

        int capacity = _buffer.Length;
        while (capacity < required)
        {
            capacity *= 2;
        }

        Array.Resize(ref _buffer, capacity);
    }
}
