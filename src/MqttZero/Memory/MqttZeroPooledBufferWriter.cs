using System.Buffers;

namespace MqttZero.Memory;

/// <summary>
/// 从缓冲池租用内存的 IBufferWriter 实现。
/// </summary>
internal sealed class MqttZeroPooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private readonly MqttZeroBufferPool _bufferPool;
    private byte[] _buffer;
    private int _written;

    /// <summary>
    /// 初始化 MqttZeroPooledBufferWriter 的新实例。
    /// </summary>
    /// <param name="bufferPool">缓冲区池。</param>
    /// <param name="minimumLength">最小长度。</param>
    public MqttZeroPooledBufferWriter(MqttZeroBufferPool bufferPool, int minimumLength)
    {
        ArgumentNullException.ThrowIfNull(bufferPool);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumLength);
        _bufferPool = bufferPool;
        _buffer = bufferPool.Rent(minimumLength);
    }

    /// <summary>
    /// WrittenMemory 的值。
    /// </summary>
    public ReadOnlyMemory<byte> WrittenMemory
    {
        get
        {
            return _buffer.AsMemory(0, _written);
        }
    }

    /// <summary>
    /// WrittenCount 的值。
    /// </summary>
    public int WrittenCount
    {
        get
        {
            return _written;
        }
    }

    /// <summary>
    /// 推进。
    /// </summary>
    /// <param name="count">数量。</param>
    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            count,
            _buffer.Length - _written,
            nameof(count));

        _written += count;
    }

    /// <summary>
    /// 获取内存。
    /// </summary>
    /// <param name="sizeHint">大小提示。</param>
    /// <returns>可直接写入的字节内存块。</returns>
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    /// <summary>
    /// 获取跨度。
    /// </summary>
    /// <param name="sizeHint">大小提示。</param>
    /// <returns>可直接写入的字节内存块。</returns>
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Dispose()
    {
        if (_buffer.Length == 0)
        {
            return;
        }

        _bufferPool.Return(_buffer);
        _buffer = [];
    }

    /// <summary>
    /// 重置。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
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

    /// <summary>
    /// 确保容量。
    /// </summary>
    /// <param name="sizeHint">大小提示。</param>
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
