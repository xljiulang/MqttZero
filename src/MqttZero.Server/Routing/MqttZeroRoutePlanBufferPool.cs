using System.Collections.Concurrent;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRoutePlanBufferPool : IDisposable
{
    private readonly ConcurrentQueue<MqttZeroRoutePlanBuffer> _buffers =
        new();
    private readonly int _shardCount;
    private int _disposed;
    private int _inUseCount;

    public MqttZeroRoutePlanBufferPool(int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        _shardCount = shardCount;
        ReturnCallback = Return;
    }

    public Action<MqttZeroRoutePlanBuffer> ReturnCallback { get; }

    public int AvailableCount
    {
        get
        {
            return _buffers.Count;
        }
    }

    public int InUseCount
    {
        get
        {
            return Volatile.Read(ref _inUseCount);
        }
    }

    public MqttZeroRoutePlanBuffer Rent()
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        MqttZeroRoutePlanBuffer buffer =
            _buffers.TryDequeue(out MqttZeroRoutePlanBuffer? pooled)
                ? pooled
                : new MqttZeroRoutePlanBuffer(_shardCount);
        Interlocked.Increment(ref _inUseCount);
        return buffer;
    }

    public void Return(MqttZeroRoutePlanBuffer buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        Interlocked.Decrement(ref _inUseCount);
        if (Volatile.Read(ref _disposed) != 0)
        {
            buffer.Dispose();
            return;
        }

        _buffers.Enqueue(buffer);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        while (_buffers.TryDequeue(out MqttZeroRoutePlanBuffer? buffer))
        {
            buffer.Dispose();
        }
    }
}
