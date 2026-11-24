using MqttZero.Protocol;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Clients;

internal sealed class MqttZeroConnectionWriter : IAsyncDisposable
{
    private const int QueueCapacity = 1024;

    private readonly List<MqttZeroQueuedMessage> _batch = new(128);
    private readonly MqttZeroQueuedMessage?[] _messages =
        new MqttZeroQueuedMessage?[QueueCapacity];
    private readonly MqttZeroClientConnection _connection;
    private readonly object _gate = new();
    private readonly MqttZeroAsyncSignal _signal = new();
    private readonly SemaphoreSlim _spaceAvailable = new(0, int.MaxValue);
    private readonly CancellationTokenSource _stopping = new();
    private Action<Exception>? _faulted;
    private int _count;
    private int _disposed;
    private int _head;
    private int _started;
    private Task _writerTask = Task.CompletedTask;

    internal MqttZeroConnectionWriter(MqttZeroClientConnection connection)
    {
        _connection = connection;
    }

    internal void Start(Action<Exception> faulted)
    {
        ArgumentNullException.ThrowIfNull(faulted);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_started != 0)
            {
                return;
            }

            _faulted = faulted;
            _started = 1;
            _writerTask = RunAsync(_stopping.Token);
        }
    }

    internal async ValueTask EnqueueBatchAsync(
        IReadOnlyList<MqttZeroQueuedMessage> messages,
        CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }

        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        while (true)
        {
            lock (_gate)
            {
                if (_count + messages.Count <= QueueCapacity)
                {
                    for (int index = 0; index < messages.Count; index++)
                    {
                        int tail = (_head + _count) % QueueCapacity;
                        _messages[tail] = messages[index];
                        _count++;
                    }

                    _signal.Signal();
                    return;
                }
            }

            await _spaceAvailable.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    internal async ValueTask StopAsync()
    {
        Task writerTask;
        bool stopStarted = false;
        lock (_gate)
        {
            if (_disposed != 0)
            {
                writerTask = _writerTask;
            }
            else
            {
                _disposed = 1;
                writerTask = _writerTask;
                stopStarted = true;
            }
        }

        if (stopStarted)
        {
            _stopping.Cancel();
            _signal.Signal();
        }

        try
        {
            await writerTask.ConfigureAwait(false);
        }
        finally
        {
            if (stopStarted)
            {
                ReleaseQueuedQos0Messages();
                _stopping.Dispose();
                _spaceAvailable.Dispose();
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        return StopAsync();
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await _signal.WaitAsync().ConfigureAwait(false);
            while (!cancellationToken.IsCancellationRequested &&
                TakeBatch())
            {
                try
                {
                    await _connection.SendPublishBatchAsync(
                        _batch,
                        cancellationToken).ConfigureAwait(false);
                    ReleaseBatchQos0Messages();
                }
                catch (OperationCanceledException)
                    when (cancellationToken.IsCancellationRequested)
                {
                    ReleaseBatchQos0Messages();
                    return;
                }
                catch (Exception exception)
                {
                    ReleaseBatchQos0Messages();
                    ReleaseQueuedQos0Messages();
                    _faulted?.Invoke(exception);
                    return;
                }
                finally
                {
                    _batch.Clear();
                }
            }
        }
    }

    private bool TakeBatch()
    {
        lock (_gate)
        {
            if (_count == 0)
            {
                return false;
            }

            MqttZeroQueuedMessage first = _messages[_head]!;
            int maximumMessages = first.Lease.Payload.Length >= 32 * 1024
                ? _count > 1
                    ? 4
                    : 1
                : 128;
            long maximumBytes = first.Lease.Payload.Length >= 32 * 1024
                ? _count > 1
                    ? 256 * 1024
                    : 64 * 1024
                : 1024 * 1024;
            if (_connection.HasPendingControlWrites)
            {
                maximumMessages = 1;
                maximumBytes = 64 * 1024;
            }

            _batch.Clear();
            long batchBytes = 0;
            while (_count > 0 &&
                _batch.Count < maximumMessages &&
                batchBytes < maximumBytes)
            {
                MqttZeroQueuedMessage message = _messages[_head]!;
                _messages[_head] = null;
                _head = (_head + 1) % QueueCapacity;
                _count--;
                _batch.Add(message);
                batchBytes += message.Lease.Payload.Length + 32;
            }

            _spaceAvailable.Release(_batch.Count);
            return true;
        }
    }

    private void ReleaseBatchQos0Messages()
    {
        for (int index = 0; index < _batch.Count; index++)
        {
            if (_batch[index].QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtMostOnce)
            {
                _batch[index].Release();
            }
        }
    }

    private void ReleaseQueuedQos0Messages()
    {
        lock (_gate)
        {
            while (_count > 0)
            {
                MqttZeroQueuedMessage message = _messages[_head]!;
                _messages[_head] = null;
                _head = (_head + 1) % QueueCapacity;
                _count--;
                if (message.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtMostOnce)
                {
                    message.Release();
                }
            }

            _head = 0;
        }
    }
}
