using MqttZero.Memory;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressControlFrame : IDisposable
{
    private readonly MqttZeroPooledBufferWriter _writer;
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

    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        _completion.TrySetException(exception);
    }

    public void Dispose()
    {
        _writer.Dispose();
    }
}
