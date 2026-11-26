using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

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

    public void Reset(int reservedLength = 0)
    {
        _position = reservedLength;
        _length = reservedLength;
    }

    public void Seek(int position)
    {
        EnsureCapacity(position);
        _position = position;
    }

    public void Advance(int count)
    {
        _position += count;
        if (_position > _length)
        {
            _length = _position;
        }
    }

    public void Clear()
    {
        Reset();
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(_position + Math.Max(sizeHint, 1));
        return _buffer.AsMemory(_position);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(_position + Math.Max(sizeHint, 1));
        return _buffer.AsSpan(_position);
    }

    public void WriteByte(byte value)
    {
        EnsureCapacity(_position + 1);
        _buffer[_position++] = value;
        _length = Math.Max(_length, _position);
    }

    public void WriteUInt16(ushort value)
    {
        Span<byte> span = GetSpan(sizeof(ushort));
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        Advance(sizeof(ushort));
    }

    public void WriteUInt32(uint value)
    {
        Span<byte> span = GetSpan(sizeof(uint));
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        Advance(sizeof(uint));
    }

    public void WriteVariableByteInteger(uint value)
    {
        Span<byte> destination = GetSpan(4);
        int count = WriteVariableByteInteger(destination, value);
        Advance(count);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        Span<byte> destination = GetSpan(value.Length);
        value.CopyTo(destination);
        Advance(value.Length);
    }

    public static int WriteVariableByteInteger(Span<byte> destination, uint value)
    {
        return MqttZeroWireFormat.WriteVariableByteInteger(destination, (int)value);
    }

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
