using System.IO.Pipelines;

namespace MqttZero.Memory;

/// <summary>
/// 限制写入字节上限的 PipeWriter 包装。
/// </summary>
internal sealed class MqttZeroLimitedPipeWriter : PipeWriter
{
    private readonly PipeWriter _inner;
    private readonly int _maximumBytes;
    private int _writtenBytes;

    /// <summary>
    /// 初始化 MqttZeroLimitedPipeWriter 的新实例。
    /// </summary>
    /// <param name="inner">对应的 inner。</param>
    /// <param name="maximumBytes">最大字节数。</param>
    public MqttZeroLimitedPipeWriter(PipeWriter inner, int maximumBytes)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentOutOfRangeException.ThrowIfNegative(maximumBytes);
        _inner = inner;
        _maximumBytes = maximumBytes;
    }

    /// <summary>
    /// 推进。
    /// </summary>
    /// <param name="bytes">字节数。</param>
    public override void Advance(int bytes)
    {
        EnsureCapacity(bytes);
        _inner.Advance(bytes);
        _writtenBytes += bytes;
    }

    /// <summary>
    /// 获取内存。
    /// </summary>
    /// <param name="sizeHint">大小提示。</param>
    /// <returns>可直接写入的字节内存块。</returns>
    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureRemainingCapacity();
        return _inner.GetMemory(sizeHint);
    }

    /// <summary>
    /// 获取跨度。
    /// </summary>
    /// <param name="sizeHint">大小提示。</param>
    /// <returns>可直接写入的字节内存块。</returns>
    public override Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureRemainingCapacity();
        return _inner.GetSpan(sizeHint);
    }

    /// <summary>
    /// 刷出。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    public override ValueTask<FlushResult> FlushAsync(
        CancellationToken cancellationToken = default)
    {
        return _inner.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// 完成。
    /// </summary>
    /// <param name="exception">异常。</param>
    public override void Complete(Exception? exception = null)
    {
        _inner.Complete(exception);
    }

    /// <summary>
    /// 完成。
    /// </summary>
    /// <param name="exception">异常。</param>
    /// <returns>表示异步操作的任务。</returns>
    public override ValueTask CompleteAsync(Exception? exception = null)
    {
        return _inner.CompleteAsync(exception);
    }

    /// <summary>
    /// Cancel待处理Flush。
    /// </summary>
    public override void CancelPendingFlush()
    {
        _inner.CancelPendingFlush();
    }

    /// <summary>
    /// 写入。
    /// </summary>
    /// <param name="source">来源。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    public override async ValueTask<FlushResult> WriteAsync(
        ReadOnlyMemory<byte> source,
        CancellationToken cancellationToken = default)
    {
        EnsureCapacity(source.Length);
        FlushResult result = await _inner.WriteAsync(source, cancellationToken)
            .ConfigureAwait(false);
        _writtenBytes += source.Length;
        return result;
    }

    /// <summary>
    /// 确保Remaining容量。
    /// </summary>
    private void EnsureRemainingCapacity()
    {
        if (_writtenBytes >= _maximumBytes)
        {
            throw new InvalidOperationException(
                "The publish payload exceeds the broker maximum packet size.");
        }
    }

    /// <summary>
    /// 确保容量。
    /// </summary>
    /// <param name="additionalBytes">additional字节数。</param>
    private void EnsureCapacity(int additionalBytes)
    {
        if (additionalBytes > _maximumBytes - _writtenBytes)
        {
            throw new InvalidOperationException(
                "The publish payload exceeds the broker maximum packet size.");
        }
    }
}
