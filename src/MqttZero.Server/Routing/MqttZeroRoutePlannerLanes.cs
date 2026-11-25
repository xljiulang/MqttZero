namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRoutePlannerLanes : IDisposable
{
    private readonly MqttZeroRoutePlanBuffer[] _buffers;
    private bool _disposed;

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

    public int Count { get; }

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
