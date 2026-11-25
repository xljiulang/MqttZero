using MqttZero.Memory;
using System.Threading.Tasks.Sources;

namespace MqttZero.Server.Egress;

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

    public MqttZeroPooledBufferWriter Writer
    {
        get
        {
            return _writer;
        }
    }

    public ReadOnlyMemory<byte> EncodedMemory
    {
        get
        {
            return _writer.WrittenMemory;
        }
    }

    public int EncodedLength
    {
        get
        {
            return _writer.WrittenCount;
        }
    }

    public ValueTask WaitAsync()
    {
        return new ValueTask(
            this,
            _completion.Version);
    }

    public void Complete()
    {
        if (Interlocked.Exchange(ref _completed, 1) == 0)
        {
            _completion.SetResult(true);
        }
    }

    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (Interlocked.Exchange(ref _completed, 1) == 0)
        {
            _completion.SetException(exception);
        }
    }

    public void Release()
    {
        if (Interlocked.Exchange(ref _released, 1) == 0)
        {
            _owner.Return(this);
        }
    }

    internal void MarkForActorRelease()
    {
        Volatile.Write(ref _releaseOnCompletion, 1);
    }

    internal void ReleaseIfActorOwned()
    {
        if (Volatile.Read(ref _releaseOnCompletion) != 0)
        {
            Release();
        }
    }

    public ValueTaskSourceStatus GetStatus(short token)
    {
        return _completion.GetStatus(token);
    }

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

    public void GetResult(short token)
    {
        _completion.GetResult(token);
    }

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

    public void Dispose()
    {
        DisposeWriter();
    }

    internal void DisposeWriter()
    {
        _writer.Dispose();
    }
}
