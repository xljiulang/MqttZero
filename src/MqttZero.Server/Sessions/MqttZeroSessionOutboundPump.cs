namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroSessionOutboundPump : IAsyncDisposable
{
    private readonly Func<CancellationToken, ValueTask> _drain;
    private readonly Action<Exception> _faulted;
    private readonly Func<ValueTask>? _stopAsync;
    private readonly MqttZeroAsyncSignal _signal = new();
    private readonly object _gate = new();
    private readonly CancellationTokenSource _stopping = new();
    private Task _pumpTask = Task.CompletedTask;
    private int _disposed;
    private int _started;

    public MqttZeroSessionOutboundPump(
        Func<CancellationToken, ValueTask> drain,
        Action<Exception> faulted)
        : this(drain, faulted, null)
    {
    }

    internal MqttZeroSessionOutboundPump(
        Func<CancellationToken, ValueTask> drain,
        Action<Exception> faulted,
        Func<ValueTask>? stopAsync)
    {
        _drain = drain ?? throw new ArgumentNullException(nameof(drain));
        _faulted = faulted ?? throw new ArgumentNullException(nameof(faulted));
        _stopAsync = stopAsync;
    }

    public void Start()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_started != 0)
            {
                return;
            }

            _started = 1;
            _pumpTask = RunAsync(_stopping.Token);
        }
    }

    public void Signal()
    {
        _signal.Signal();
    }

    public async ValueTask StopAsync()
    {
        Task pumpTask;
        bool stopStarted = false;
        lock (_gate)
        {
            if (_disposed != 0)
            {
                pumpTask = _pumpTask;
            }
            else
            {
                _disposed = 1;
                pumpTask = _pumpTask;
                stopStarted = true;
            }
        }

        if (stopStarted)
        {
            _stopping.Cancel();
            _signal.Signal();
            if (_stopAsync != null)
            {
                await _stopAsync().ConfigureAwait(false);
            }
        }

        try
        {
            await pumpTask.ConfigureAwait(false);
        }
        finally
        {
            if (stopStarted)
            {
                _stopping.Dispose();
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        return StopAsync();
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await _signal.WaitAsync().ConfigureAwait(false);
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

                await _drain(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            try
            {
                _faulted(exception);
            }
            catch (Exception)
            {
            }
        }
    }
}
