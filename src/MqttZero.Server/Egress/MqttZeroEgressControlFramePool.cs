using MqttZero.Memory;
using System.Collections.Concurrent;

namespace MqttZero.Server.Egress;

/// <summary>
/// 出站控制帧的对象池。
/// </summary>
internal sealed class MqttZeroEgressControlFramePool : IDisposable
{
    private const int MaximumRetainedFrameCount = 64;

    private readonly MqttZeroBufferPool _bufferPool;
    private readonly ConcurrentQueue<MqttZeroEgressControlFrame> _frames = new();
    private int _disposed;
    private int _retainedCount;

    /// <summary>
    /// 初始化 MqttZeroEgressControlFramePool 的新实例。
    /// </summary>
    /// <param name="bufferPool">缓冲区池。</param>
    public MqttZeroEgressControlFramePool(MqttZeroBufferPool bufferPool)
    {
        _bufferPool = bufferPool ??
            throw new ArgumentNullException(nameof(bufferPool));
    }

    public int RetainedCount
    {
        get
        {
            return Volatile.Read(ref _retainedCount);
        }
    }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    /// <returns>对应的结果。</returns>
    public MqttZeroEgressControlFrame Rent(int minimumLength)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        while (_frames.TryDequeue(out MqttZeroEgressControlFrame? frame))
        {
            Interlocked.Decrement(ref _retainedCount);
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

    /// <summary>
    /// 归还。
    /// </summary>
    /// <param name="frame">帧。</param>
    public void Return(MqttZeroEgressControlFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        if (Volatile.Read(ref _disposed) != 0)
        {
            frame.Dispose();
            return;
        }

        if (Interlocked.Increment(ref _retainedCount) >
            MaximumRetainedFrameCount)
        {
            Interlocked.Decrement(ref _retainedCount);
            frame.Dispose();
            return;
        }

        _frames.Enqueue(frame);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        while (_frames.TryDequeue(out MqttZeroEgressControlFrame? frame))
        {
            Interlocked.Decrement(ref _retainedCount);
            frame.Dispose();
        }
    }
}
