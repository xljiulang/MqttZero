namespace MqttZero.Server.Scheduling;

/// <summary>
/// 跨线程调用分片并等待结果的封装。
/// </summary>
internal sealed class MqttZeroShardInvocation<T> : MqttZeroShardWork
{
    private readonly Func<CancellationToken, ValueTask<T>> _operation;
    private readonly TaskCompletionSource<T> _completion = new(
        TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>
    /// 初始化 MqttZeroShardInvocation 的新实例。
    /// </summary>
    /// <param name="operation">对应的 operation。</param>
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

    /// <summary>
    /// 执行。
    /// </summary>
    /// <param name="shard">分片。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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
