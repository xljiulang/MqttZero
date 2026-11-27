namespace MqttZero.Server.Broker;

/// <summary>
/// Tracks the outstanding work of one connection phase and hands out the drain
/// token that belongs to the current episode.
/// </summary>
internal sealed class MqttZeroPendingDrain
{
    private readonly object _gate = new();
    private TaskCompletionSource? _completion;
    private int _countMirror;
    private int _outstanding;

    /// <summary>
    /// Gets the number of outstanding work items.
    /// </summary>
    public int Count
    {
        get
        {
            return Volatile.Read(ref _countMirror);
        }
    }

    /// <summary>
    /// Registers one work item and returns the token that owns its completion.
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
    /// Completes one work item.
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
    /// Waits until no work item is outstanding.
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
