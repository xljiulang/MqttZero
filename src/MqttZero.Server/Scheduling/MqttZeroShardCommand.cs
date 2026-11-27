using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroShardCommand<T> :
    MqttZeroShardWork,
    IValueTaskSource<T>
{
    private const int IdleState = 0;
    private const int PreparedState = 1;
    private const int ExecutingState = 2;
    private const int CompletedState = 3;

    private CancellationToken _cancellationToken;
    private ManualResetValueTaskSourceCore<T> _completion = new()
    {
        RunContinuationsAsynchronously = true
    };
    private Func<CancellationToken, ValueTask<T>>? _operation;
    private int _state;
    private short _version;

    internal ValueTask<T> Completion
    {
        get
        {
            return new ValueTask<T>(this, _version);
        }
    }

    internal void Prepare(
        Func<CancellationToken, ValueTask<T>> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operation);
        if (Interlocked.CompareExchange(
                ref _state,
                PreparedState,
                IdleState) != IdleState)
        {
            throw new InvalidOperationException(
                "The shard command is already in use.");
        }

        _operation = operation;
        _cancellationToken = cancellationToken;
    }

    internal void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (Interlocked.Exchange(
                ref _state,
                CompletedState) == CompletedState)
        {
            return;
        }

        ClearOperation();
        _completion.SetException(exception);
    }

    internal ValueTask<T> ExecuteInlineAsync()
    {
        if (Interlocked.CompareExchange(
                ref _state,
                ExecutingState,
                PreparedState) != PreparedState)
        {
            throw new InvalidOperationException(
                "The shard command is not prepared.");
        }

        Func<CancellationToken, ValueTask<T>> operation =
            _operation ??
            throw new InvalidOperationException(
                "The shard command operation is unavailable.");
        ValueTask<T> task;
        try
        {
            task = operation(_cancellationToken);
        }
        catch (Exception exception)
        {
            ClearOperation();
            Reset();
            return ValueTask.FromException<T>(exception);
        }

        if (task.IsCompletedSuccessfully)
        {
            T result = task.Result;
            ClearOperation();
            Reset();
            return ValueTask.FromResult(result);
        }

        return AwaitInlineAsync(task);
    }

    public override ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(shard);
        if (Interlocked.CompareExchange(
                ref _state,
                ExecutingState,
                PreparedState) != PreparedState)
        {
            throw new InvalidOperationException(
                "The shard command is not prepared.");
        }

        Func<CancellationToken, ValueTask<T>> operation =
            _operation ??
            throw new InvalidOperationException(
                "The shard command operation is unavailable.");
        ValueTask<T> task;
        try
        {
            task = operation(_cancellationToken);
        }
        catch (Exception exception)
        {
            Complete(exception);
            return ValueTask.CompletedTask;
        }

        if (task.IsCompletedSuccessfully)
        {
            Complete(task.Result);
            return ValueTask.CompletedTask;
        }

        return AwaitAndCompleteAsync(task);
    }

    public override void Release()
    {
    }

    public T GetResult(short token)
    {
        try
        {
            return _completion.GetResult(token);
        }
        finally
        {
            Reset();
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

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<T> AwaitInlineAsync(ValueTask<T> task)
    {
        try
        {
            return await task.ConfigureAwait(false);
        }
        finally
        {
            ClearOperation();
            Reset();
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask AwaitAndCompleteAsync(ValueTask<T> task)
    {
        try
        {
            T result = await task.ConfigureAwait(false);
            Complete(result);
        }
        catch (Exception exception)
        {
            Complete(exception);
        }
    }

    private void Complete(T result)
    {
        if (Interlocked.Exchange(
                ref _state,
                CompletedState) == CompletedState)
        {
            return;
        }

        ClearOperation();
        _completion.SetResult(result);
    }

    private void Complete(Exception exception)
    {
        if (Interlocked.Exchange(
                ref _state,
                CompletedState) == CompletedState)
        {
            return;
        }

        ClearOperation();
        _completion.SetException(exception);
    }

    private void ClearOperation()
    {
        _operation = null;
        _cancellationToken = default;
    }

    private void Reset()
    {
        _completion.Reset();
        _version = _completion.Version;
        Volatile.Write(ref _state, IdleState);
    }
}
