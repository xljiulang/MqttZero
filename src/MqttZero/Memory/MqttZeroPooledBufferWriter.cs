using System.Buffers;

namespace MqttZero.Memory;

internal sealed class MqttZeroPooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private readonly MqttZeroBufferPool _bufferPool;
    private byte[] _buffer;
    private int _written;

    public MqttZeroPooledBufferWriter(MqttZeroBufferPool bufferPool, int minimumLength)
    {
        ArgumentNullException.ThrowIfNull(bufferPool);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumLength);
        _bufferPool = bufferPool;
        _buffer = bufferPool.Rent(minimumLength);
    }

    public ReadOnlyMemory<byte> WrittenMemory
    {
        get
        {
            return _buffer.AsMemory(0, _written);
        }
    }

    public int WrittenCount
    {
        get
        {
            return _written;
        }
    }

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            count,
            _buffer.Length - _written,
            nameof(count));

        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    public void Dispose()
    {
        if (_buffer.Length == 0)
        {
            return;
        }

        _bufferPool.Return(_buffer);
        _buffer = [];
    }

    public void Reset(int minimumLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumLength);
        if (_buffer.Length < minimumLength)
        {
            byte[] previous = _buffer;
            _buffer = _bufferPool.Rent(minimumLength);
            _bufferPool.Return(previous);
        }

        _written = 0;
    }

    private void EnsureCapacity(int sizeHint)
    {
        int required = Math.Max(sizeHint, 1);
        if (required > _buffer.Length - _written)
        {
            throw new InvalidOperationException(
                "The encoded MQTT packet exceeded its precomputed length.");
        }
    }
}
