using MqttZero.Memory;
using System.Threading.Tasks.Sources;

namespace MqttZero.Server.Egress;

/// <summary>
/// 编码完成的出站控制帧。
/// </summary>
internal sealed class MqttZeroEgressControlFrame : IValueTaskSource, IDisposable
{
    private readonly MqttZeroEgressControlFramePool _owner;
    private readonly MqttZeroPooledBufferWriter _writer;
    private ManualResetValueTaskSourceCore<bool> _completion = new()
    {
        RunContinuationsAsynchronously = true
    };
    private int _completed;
    private int _releaseOnCompletion;
    private int _released;

    /// <summary>
    /// 初始化 MqttZeroEgressControlFrame 的新实例。
    /// </summary>
    /// <param name="owner">持有者。</param>
    /// <param name="bufferPool">缓冲区池。</param>
    /// <param name="encodedLength">encoded长度。</param>
    internal MqttZeroEgressControlFrame(
        MqttZeroEgressControlFramePool owner,
        MqttZeroBufferPool bufferPool,
        int encodedLength)
    {
        _owner = owner ??
            throw new ArgumentNullException(nameof(owner));
        _writer = new MqttZeroPooledBufferWriter(
            bufferPool,
            encodedLength);
    }

    /// <summary>
    /// Writer 的值。
    /// </summary>
    public MqttZeroPooledBufferWriter Writer
    {
        get
        {
            return _writer;
        }
    }

    /// <summary>
    /// EncodedMemory 的值。
    /// </summary>
    public ReadOnlyMemory<byte> EncodedMemory
    {
        get
        {
            return _writer.WrittenMemory;
        }
    }

    /// <summary>
    /// EncodedLength 的值。
    /// </summary>
    public int EncodedLength
    {
        get
        {
            return _writer.WrittenCount;
        }
    }

    /// <summary>
    /// 等待。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask WaitAsync()
    {
        return new ValueTask(
            this,
            _completion.Version);
    }

    /// <summary>
    /// 完成。
    /// </summary>
    public void Complete()
    {
        if (Interlocked.Exchange(ref _completed, 1) == 0)
        {
            _completion.SetResult(true);
        }
    }

    /// <summary>
    /// 标记失败。
    /// </summary>
    /// <param name="exception">异常。</param>
    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (Interlocked.Exchange(ref _completed, 1) == 0)
        {
            _completion.SetException(exception);
        }
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0)
        {
            _owner.Return(this);
        }
    }

    /// <summary>
    /// 标记为由 Egress Actor 释放。
    /// </summary>
    internal void MarkForActorRelease()
    {
        Volatile.Write(ref _releaseOnCompletion, 1);
    }

    /// <summary>
    /// 释放IfActorOwned。
    /// </summary>
    internal void ReleaseIfActorOwned()
    {
        if (Volatile.Read(ref _releaseOnCompletion) != 0)
        {
            Release();
        }
    }

    /// <summary>
    /// 获取状态。
    /// </summary>
    /// <param name="token">令牌。</param>
    /// <returns>对应的结果。</returns>
    public ValueTaskSourceStatus GetStatus(short token)
    {
        return _completion.GetStatus(token);
    }

    /// <summary>
    /// 执行 OnCompleted 对应的处理。
    /// </summary>
    /// <param name="continuation">对应的 continuation。</param>
    /// <param name="state">状态。</param>
    /// <param name="token">令牌。</param>
    /// <param name="flags">标志。</param>
    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
        _completion.OnCompleted(
            continuation,
            state,
            token,
            flags);
    }

    /// <summary>
    /// 获取结果。
    /// </summary>
    /// <param name="token">令牌。</param>
    public void GetResult(short token)
    {
        _completion.GetResult(token);
    }

    /// <summary>
    /// 尝试Reset。
    /// </summary>
    /// <param name="minimumLength">最小长度。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    internal bool TryReset(int minimumLength)
    {
        try
        {
            _writer.Reset(minimumLength);
        }
        catch (MqttZeroBufferPoolExhaustedException)
        {
            return false;
        }

        _completion.Reset();
        Volatile.Write(ref _completed, 0);
        Volatile.Write(ref _releaseOnCompletion, 0);
        Volatile.Write(ref _released, 0);
        return true;
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Dispose()
    {
        DisposeWriter();
    }

    /// <summary>
    /// 释放写入器。
    /// </summary>
    internal void DisposeWriter()
    {
        _writer.Dispose();
    }
}
