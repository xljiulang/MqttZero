using System.Runtime.CompilerServices;
using System.Threading.Tasks.Sources;

namespace MqttZero.Server.Scheduling;

/// <summary>
/// 在分片上执行的命令封装。
/// </summary>
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

    /// <summary>
    /// Completion 的值。
    /// </summary>
    internal ValueTask<T> Completion
    {
        get
        {
            return new ValueTask<T>(this, _version);
        }
    }

    /// <summary>
    /// 执行 Prepare 对应的处理。
    /// </summary>
    /// <param name="operation">对应的 operation。</param>
    /// <param name="cancellationToken">取消令牌。</param>
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

    /// <summary>
    /// 标记失败。
    /// </summary>
    /// <param name="exception">异常。</param>
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

    /// <summary>
    /// 执行Inline。
    /// </summary>
    /// <returns>异步操作的结果。</returns>
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

    /// <summary>
    /// 执行。
    /// </summary>
    /// <param name="shard">分片。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 释放。
    /// </summary>
    public override void Release()
    {
    }

    /// <summary>
    /// 获取结果。
    /// </summary>
    /// <param name="token">令牌。</param>
    /// <returns>对应的结果。</returns>
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
    /// 内联等待命令。
    /// </summary>
    /// <param name="task">对应的 task。</param>
    /// <returns>异步操作的结果。</returns>
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

    /// <summary>
    /// 等待并完成命令。
    /// </summary>
    /// <param name="task">对应的 task。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 完成。
    /// </summary>
    /// <param name="result">结果。</param>
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

    /// <summary>
    /// 完成。
    /// </summary>
    /// <param name="exception">异常。</param>
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

    /// <summary>
    /// 清空Operation。
    /// </summary>
    private void ClearOperation()
    {
        _operation = null;
        _cancellationToken = default;
    }

    /// <summary>
    /// 重置。
    /// </summary>
    private void Reset()
    {
        _completion.Reset();
        _version = _completion.Version;
        Volatile.Write(ref _state, IdleState);
    }
}
