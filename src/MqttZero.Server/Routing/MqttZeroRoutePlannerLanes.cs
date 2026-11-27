namespace MqttZero.Server.Routing;

/// <summary>
/// 每个分片私有的一组路由计划缓冲区。
/// </summary>
internal sealed class MqttZeroRoutePlannerLanes : IDisposable
{
    private readonly MqttZeroRoutePlanBuffer[] _buffers;
    private bool _disposed;

    /// <summary>
    /// 初始化 MqttZeroRoutePlannerLanes 的新实例。
    /// </summary>
    /// <param name="shardCount">分片数量。</param>
    public MqttZeroRoutePlannerLanes(int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        Count = shardCount;
        _buffers = new MqttZeroRoutePlanBuffer[shardCount];
        for (int shardId = 0; shardId < shardCount; shardId++)
        {
            _buffers[shardId] = new MqttZeroRoutePlanBuffer(shardCount);
        }
    }

    /// <summary>
    /// 数量。
    /// </summary>
    public int Count { get; }

    /// <summary>
    /// 按索引获取元素。
    /// </summary>
    public MqttZeroRoutePlanBuffer this[int shardId]
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if ((uint)shardId >= (uint)Count)
            {
                throw new ArgumentOutOfRangeException(nameof(shardId));
            }

            return _buffers[shardId];
        }
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        for (int shardId = 0; shardId < _buffers.Length; shardId++)
        {
            _buffers[shardId].Dispose();
        }

        Array.Clear(_buffers);
    }
}
