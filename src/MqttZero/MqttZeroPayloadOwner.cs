using System.Buffers;

namespace MqttZero;

/// <summary>
/// 池化负载的所有者抽象。
/// </summary>
public abstract class MqttZeroPayloadOwner : IAsyncDisposable
{
    private int _disposed;

    /// <summary>
    /// Gets the payload sequence owned by this instance.
    /// </summary>
    public abstract ReadOnlySequence<byte> Payload { get; }

    /// <summary>
    /// Releases the payload resources.
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
    /// Releases resources owned by a derived payload owner.
    /// </summary>
    /// <returns>A task representing the asynchronous disposal operation.</returns>
    protected virtual ValueTask DisposeAsyncCore()
    {
        return ValueTask.CompletedTask;
    }
}
