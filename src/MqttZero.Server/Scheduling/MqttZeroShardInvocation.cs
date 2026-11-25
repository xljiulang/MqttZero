namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroShardInvocation<T> : MqttZeroShardWork
{
    private readonly Func<CancellationToken, ValueTask<T>> _operation;
    private readonly TaskCompletionSource<T> _completion = new(
        TaskCreationOptions.RunContinuationsAsynchronously);

    public MqttZeroShardInvocation(
        Func<CancellationToken, ValueTask<T>> operation)
    {
        _operation = operation ?? throw new ArgumentNullException(
            nameof(operation));
    }

    public Task<T> Task
    {
        get
        {
            return _completion.Task;
        }
    }

    public override async ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        try
        {
            T result = await _operation(cancellationToken)
                .ConfigureAwait(false);
            _completion.TrySetResult(result);
        }
        catch (Exception exception)
        {
            _completion.TrySetException(exception);
        }
    }
}
