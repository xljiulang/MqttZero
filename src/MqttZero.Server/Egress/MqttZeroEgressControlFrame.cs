using MqttZero.Memory;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressControlFrame : IDisposable
{
    private readonly Func<CancellationToken, ValueTask>? _operation;
    private readonly MqttZeroPooledBufferWriter? _writer;
    private readonly TaskCompletionSource _completion = new(
        TaskCreationOptions.RunContinuationsAsynchronously);

    public MqttZeroEgressControlFrame(
        MqttZeroBufferPool bufferPool,
        int encodedLength)
    {
        _writer = new MqttZeroPooledBufferWriter(
            bufferPool,
            encodedLength);
    }

    private MqttZeroEgressControlFrame(
        Func<CancellationToken, ValueTask> operation)
    {
        _operation = operation;
    }

    public static MqttZeroEgressControlFrame ForOperation(
        Func<CancellationToken, ValueTask> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return new MqttZeroEgressControlFrame(operation);
    }

    public bool IsOperation
    {
        get
        {
            return _operation != null;
        }
    }

    public MqttZeroPooledBufferWriter Writer
    {
        get
        {
            return _writer!;
        }
    }

    public ReadOnlyMemory<byte> EncodedMemory
    {
        get
        {
            return _writer!.WrittenMemory;
        }
    }

    public Task Completion
    {
        get
        {
            return _completion.Task;
        }
    }

    public void Complete()
    {
        _completion.TrySetResult();
    }

    public ValueTask ExecuteAsync(CancellationToken cancellationToken)
    {
        return _operation == null
            ? ValueTask.CompletedTask
            : _operation(cancellationToken);
    }

    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        _completion.TrySetException(exception);
    }

    public void Dispose()
    {
        _writer?.Dispose();
    }
}
