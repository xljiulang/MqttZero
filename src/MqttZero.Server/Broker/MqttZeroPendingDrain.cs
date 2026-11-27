namespace MqttZero.Server.Broker;

/// <summary>
/// 记录当前连接阶段的待完成数量并发放排空令牌。
/// </summary>
internal sealed class MqttZeroPendingDrain
{
    private readonly object _gate = new();
    private TaskCompletionSource? _completion;
    private int _countMirror;
    private int _outstanding;

    /// <summary>
    /// 获取尚未完成的工作项数量。
    /// </summary>
    public int Count
    {
        get
        {
            return Volatile.Read(ref _countMirror);
        }
    }

    /// <summary>
    /// 登记一个工作项并返回负责完成它的令牌。
    /// </summary>
    /// <returns>The drain token of the current episode.</returns>
    public TaskCompletionSource Begin()
    {
        lock (_gate)
        {
            _outstanding++;
            Volatile.Write(ref _countMirror, _outstanding);
            _completion ??= new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously);
            return _completion;
        }
    }

    /// <summary>
    /// 完成一个工作项。
    /// </summary>
    /// <param name="token">The token returned by <see cref="Begin"/>.</param>
    public void Complete(TaskCompletionSource token)
    {
        ArgumentNullException.ThrowIfNull(token);
        TaskCompletionSource? completed = null;
        lock (_gate)
        {
            if (!ReferenceEquals(_completion, token) ||
                _outstanding == 0)
            {
                return;
            }

            _outstanding--;
            Volatile.Write(ref _countMirror, _outstanding);
            if (_outstanding == 0)
            {
                completed = token;
                _completion = null;
            }
        }

        completed?.TrySetResult();
    }

    /// <summary>
    /// 等待所有工作项完成。
    /// </summary>
    /// <returns>A task that completes when the drain is idle.</returns>
    public async ValueTask WaitAsync()
    {
        while (true)
        {
            Task? pending;
            lock (_gate)
            {
                if (_outstanding == 0)
                {
                    return;
                }

                pending = _completion?.Task;
            }

            if (pending != null)
            {
                await pending.ConfigureAwait(false);
            }
        }
    }
}
