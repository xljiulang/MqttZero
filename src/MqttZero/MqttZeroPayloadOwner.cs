using System.Buffers;

namespace MqttZero;

/// <summary>
/// 池化负载的所有者抽象。
/// </summary>
public abstract class MqttZeroPayloadOwner : IAsyncDisposable
{
    private int _disposed;

    /// <summary>
    /// 获取该实例持有的负载序列。
    /// </summary>
    public abstract ReadOnlySequence<byte> Payload { get; }

    /// <summary>
    /// 释放负载资源。
    /// </summary>
    /// <returns>A task representing the asynchronous disposal operation.</returns>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return ValueTask.CompletedTask;
        }

        GC.SuppressFinalize(this);
        return DisposeAsyncCore();
    }

    /// <summary>
    /// 释放派生负载持有者持有的资源。
    /// </summary>
    /// <returns>A task representing the asynchronous disposal operation.</returns>
    protected virtual ValueTask DisposeAsyncCore()
    {
        return ValueTask.CompletedTask;
    }
}
