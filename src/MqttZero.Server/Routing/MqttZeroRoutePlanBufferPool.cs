using System.Collections.Concurrent;

namespace MqttZero.Server.Routing;

/// <summary>
/// 路由计划缓冲区的对象池。
/// </summary>
internal sealed class MqttZeroRoutePlanBufferPool : IDisposable
{
    private readonly ConcurrentQueue<MqttZeroRoutePlanBuffer> _buffers =
        new();
    private readonly int _maximumBufferCount;
    private readonly long _maximumRetainedBytes;
    private readonly int _shardCount;
    private int _availableCount;
    private int _disposed;
    private int _inUseCount;
    private long _retainedBytes;

    /// <summary>
    /// 初始化 MqttZeroRoutePlanBufferPool 的新实例。
    /// </summary>
    /// <param name="shardCount">分片数量。</param>
    /// <param name="maxBufferedBytes">maxBuffered字节数。</param>
    public MqttZeroRoutePlanBufferPool(
        int shardCount,
        long maxBufferedBytes = 512L * 1024 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBufferedBytes);
        _shardCount = shardCount;
        _maximumBufferCount = Math.Clamp(shardCount * 2, 8, 64);
        _maximumRetainedBytes = Math.Clamp(
            maxBufferedBytes / 8,
            8L * 1024 * 1024,
            64L * 1024 * 1024);
        ReturnCallback = Return;
    }

    /// <summary>
    /// 归还回调。
    /// </summary>
    public Action<MqttZeroRoutePlanBuffer> ReturnCallback { get; }

    public int AvailableCount
    {
        get
        {
            return Volatile.Read(ref _availableCount);
        }
    }

    public int InUseCount
    {
        get
        {
            return Volatile.Read(ref _inUseCount);
        }
    }

    /// <summary>
    /// 获取快照。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroRoutePlanBufferPoolSnapshot GetSnapshot()
    {
        return new MqttZeroRoutePlanBufferPoolSnapshot(
            AvailableCount,
            InUseCount,
            Volatile.Read(ref _retainedBytes));
    }

    /// <summary>
    /// 判断是否需要保留缓冲区。
    /// </summary>
    /// <param name="availableCount">available数量。</param>
    /// <param name="retainedBytes">保留字节数。</param>
    /// <param name="incomingBytes">incoming字节数。</param>
    /// <param name="maximumBufferCount">最大缓冲区数量。</param>
    /// <param name="maximumRetainedBytes">最大保留字节数。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    internal static bool ShouldRetainBuffer(
        int availableCount,
        long retainedBytes,
        long incomingBytes,
        int maximumBufferCount,
        long maximumRetainedBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(availableCount);
        ArgumentOutOfRangeException.ThrowIfNegative(retainedBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(incomingBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(
            maximumBufferCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(
            maximumRetainedBytes);
        return availableCount < maximumBufferCount &&
            retainedBytes <= maximumRetainedBytes - incomingBytes;
    }

    /// <summary>
    /// 租用。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public MqttZeroRoutePlanBuffer Rent()
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        MqttZeroRoutePlanBuffer buffer =
            _buffers.TryDequeue(out MqttZeroRoutePlanBuffer? pooled)
                ? pooled
                : new MqttZeroRoutePlanBuffer(_shardCount);
        if (pooled != null)
        {
            Interlocked.Decrement(ref _availableCount);
        }

        if (pooled is null)
        {
            Interlocked.Add(
                ref _retainedBytes,
                buffer.GetRetainedByteEstimate());
        }

        Interlocked.Increment(ref _inUseCount);
        return buffer;
    }

    /// <summary>
    /// 归还。
    /// </summary>
    /// <param name="buffer">缓冲区。</param>
    public void Return(MqttZeroRoutePlanBuffer buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        Interlocked.Decrement(ref _inUseCount);
        long retainedBytes = Volatile.Read(ref _retainedBytes);
        long bufferBytes = buffer.GetRetainedByteEstimate();
        if (Volatile.Read(ref _disposed) != 0 ||
            !ShouldRetainBuffer(
                AvailableCount,
                retainedBytes,
                bufferBytes,
                _maximumBufferCount,
                _maximumRetainedBytes))
        {
            Interlocked.Add(
                ref _retainedBytes,
                -bufferBytes);
            buffer.Dispose();
            return;
        }

        _buffers.Enqueue(buffer);
        Interlocked.Increment(ref _availableCount);
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

        while (_buffers.TryDequeue(out MqttZeroRoutePlanBuffer? buffer))
        {
            Interlocked.Decrement(ref _availableCount);
            Interlocked.Add(
                ref _retainedBytes,
                -buffer.GetRetainedByteEstimate());
            buffer.Dispose();
        }
    }
}
