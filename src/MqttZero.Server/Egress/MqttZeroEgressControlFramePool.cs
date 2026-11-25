using MqttZero.Memory;
using System.Collections.Concurrent;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressControlFramePool : IDisposable
{
    private readonly MqttZeroBufferPool _bufferPool;
    private readonly ConcurrentQueue<MqttZeroEgressControlFrame> _frames = new();
    private int _disposed;

    public MqttZeroEgressControlFramePool(MqttZeroBufferPool bufferPool)
    {
        _bufferPool = bufferPool ??
            throw new ArgumentNullException(nameof(bufferPool));
    }

    public MqttZeroEgressControlFrame Rent(int minimumLength)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        while (_frames.TryDequeue(out MqttZeroEgressControlFrame? frame))
        {
            if (frame.TryReset(minimumLength))
            {
                return frame;
            }

            frame.Dispose();
        }

        return new MqttZeroEgressControlFrame(
            this,
            _bufferPool,
            minimumLength);
    }

    public void Return(MqttZeroEgressControlFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        if (Volatile.Read(ref _disposed) != 0)
        {
            frame.Dispose();
            return;
        }

        _frames.Enqueue(frame);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        while (_frames.TryDequeue(out MqttZeroEgressControlFrame? frame))
        {
            frame.Dispose();
        }
    }
}
