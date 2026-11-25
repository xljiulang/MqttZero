using MqttZero.Server.Scheduling;
using System.IO.Pipelines;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressActor : IAsyncDisposable
{
    private const int MaximumControlQueueLength = 1024;
    private const int CoalescePassCount = 1;

    private readonly Queue<MqttZeroEgressControlFrame> _controlQueue = new();
    private readonly List<MqttZeroEgressControlFrame> _controlBatch = [];
    private readonly Func<
        IReadOnlyList<MqttZeroEgressControlFrame>,
        CancellationToken,
        ValueTask> _writeControls;
    private readonly Func<CancellationToken, ValueTask<bool>> _drainData;
    private readonly Action<Exception> _faulted;
    private readonly MqttZeroEventLoop _shard;
    private readonly TaskCompletionSource _stopped = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly MqttZeroEgressWork _work;
    private int _coalesceRemaining;
    private int _dataSignaled;
    private int _flushPending;
    private int _scheduled;
    private int _stopping;
    private int _state = (int)MqttZeroEgressActorState.Idle;

    public MqttZeroEgressActor(
        MqttZeroEventLoop shard,
        Func<
            IReadOnlyList<MqttZeroEgressControlFrame>,
            CancellationToken,
            ValueTask> writeControls,
        Func<CancellationToken, ValueTask<bool>> drainData,
        Action<Exception> faulted)
    {
        _shard = shard ?? throw new ArgumentNullException(nameof(shard));
        _writeControls = writeControls ??
            throw new ArgumentNullException(nameof(writeControls));
        _drainData = drainData ??
            throw new ArgumentNullException(nameof(drainData));
        _faulted = faulted ?? throw new ArgumentNullException(nameof(faulted));
        _work = new MqttZeroEgressWork(this);
    }

    public MqttZeroEgressActorState State
    {
        get
        {
            return (MqttZeroEgressActorState)Volatile.Read(
                ref _state);
        }
    }

    internal bool IsStopping
    {
        get
        {
            return Volatile.Read(ref _stopping) != 0;
        }
    }

    public void Signal()
    {
        SignalCore(allowCoalesce: true);
    }

    private void SignalCore(bool allowCoalesce)
    {
        if (Volatile.Read(ref _stopping) != 0)
        {
            return;
        }

        Volatile.Write(
            ref _coalesceRemaining,
            allowCoalesce ? CoalescePassCount : 0);
        if (Volatile.Read(ref _flushPending) != 0)
        {
            Volatile.Write(ref _dataSignaled, 1);
            return;
        }

        Volatile.Write(ref _dataSignaled, 1);
        Schedule();
    }

    internal bool BeginFlush(ValueTask<FlushResult> flush)
    {
        if (flush.IsCompletedSuccessfully)
        {
            FlushResult result = flush.GetAwaiter().GetResult();
            if (result.IsCompleted)
            {
                throw new IOException(
                    "The MQTT output writer completed during flush.");
            }

            return true;
        }

        if (Interlocked.Exchange(ref _flushPending, 1) != 0)
        {
            throw new InvalidOperationException(
                "The connection already has a pending flush.");
        }

        Volatile.Write(
            ref _state,
            (int)MqttZeroEgressActorState.FlushPending);
        _ = new MqttZeroEgressFlushOperation(
            flush,
            OnFlushCompleted).RunAsync();
        return false;
    }

    public async ValueTask PostControlAsync(
        MqttZeroEgressControlFrame frame,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(frame);
        ThrowIfStopping();
        cancellationToken.ThrowIfCancellationRequested();
        if (_shard.IsCurrent)
        {
            await ExecuteControlDirectAsync(
                frame,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        await _shard.InvokeAsync(
            _ =>
            {
                EnqueueControl(frame);
                return ValueTask.FromResult(true);
            },
            CancellationToken.None).ConfigureAwait(false);
        await frame.WaitAsync().ConfigureAwait(false);
    }

    public async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) == 0)
        {
            Volatile.Write(
                ref _state,
                (int)MqttZeroEgressActorState.Stopping);
            FailQueuedControls(
                new ObjectDisposedException(nameof(MqttZeroEgressActor)));
            if (Volatile.Read(ref _scheduled) == 0 &&
                Volatile.Read(ref _flushPending) == 0)
            {
                Volatile.Write(
                    ref _state,
                    (int)MqttZeroEgressActorState.Stopped);
                _stopped.TrySetResult();
            }
        }

        await _stopped.Task.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    internal async ValueTask ExecuteAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref _stopping) != 0)
        {
            return;
        }

        Volatile.Write(
            ref _state,
            (int)MqttZeroEgressActorState.Draining);
        Volatile.Write(ref _dataSignaled, 0);
        try
        {
            while (_controlQueue.Count > 0)
            {
                _controlBatch.Clear();
                while (_controlQueue.Count > 0)
                {
                    _controlBatch.Add(_controlQueue.Dequeue());
                }

                await RunControlBatchAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            if (Volatile.Read(ref _stopping) == 0)
            {
                int coalesceRemaining = Interlocked.Decrement(
                    ref _coalesceRemaining);
                if (coalesceRemaining >= 0)
                {
                    Volatile.Write(ref _dataSignaled, 1);
                }
                else
                {
                    bool hasMoreData = await _drainData(cancellationToken)
                        .ConfigureAwait(false);
                    if (hasMoreData)
                    {
                        Volatile.Write(ref _dataSignaled, 1);
                    }
                }
            }
        }
        catch (Exception exception)
        {
            FailQueuedControls(exception);
            _faulted(exception);
        }
        finally
        {
            Volatile.Write(
                ref _state,
                Volatile.Read(ref _stopping) != 0
                    ? (int)MqttZeroEgressActorState.Stopping
                    : Volatile.Read(ref _flushPending) != 0
                        ? (int)MqttZeroEgressActorState.FlushPending
                        : (int)MqttZeroEgressActorState.Idle);
        }
    }

    internal void OnWorkReleased()
    {
        Volatile.Write(ref _scheduled, 0);
        Thread.MemoryBarrier();
        if (Volatile.Read(ref _stopping) != 0)
        {
            if (_controlQueue.Count == 0 &&
                Volatile.Read(ref _flushPending) == 0)
            {
                Volatile.Write(
                    ref _state,
                    (int)MqttZeroEgressActorState.Stopped);
                _stopped.TrySetResult();
            }
            else
            {
                Schedule();
            }

            return;
        }

        if (Volatile.Read(ref _flushPending) == 0 &&
            (_controlQueue.Count > 0 ||
                Volatile.Read(ref _dataSignaled) != 0))
        {
            Schedule();
        }
    }

    private void OnFlushCompleted(Exception? exception)
    {
        Volatile.Write(ref _flushPending, 0);
        if (exception != null)
        {
            FailQueuedControls(exception);
            _faulted(exception);
            Volatile.Write(ref _stopping, 1);
            Volatile.Write(
                ref _state,
                (int)MqttZeroEgressActorState.Stopping);
        }

        if (Volatile.Read(ref _stopping) != 0)
        {
            Volatile.Write(
                ref _state,
                (int)MqttZeroEgressActorState.Stopped);
            _stopped.TrySetResult();
            return;
        }

        Volatile.Write(
            ref _state,
            (int)MqttZeroEgressActorState.Idle);
        SignalCore(allowCoalesce: false);
    }

    private async ValueTask RunControlBatchAsync(
        CancellationToken cancellationToken)
    {
        try
        {
            await _writeControls(_controlBatch, cancellationToken)
                .ConfigureAwait(false);
            for (int index = 0; index < _controlBatch.Count; index++)
            {
                MqttZeroEgressControlFrame frame = _controlBatch[index];
                frame.Complete();
            }
        }
        catch (Exception exception)
        {
            for (int index = 0; index < _controlBatch.Count; index++)
            {
                MqttZeroEgressControlFrame frame = _controlBatch[index];
                frame.Fail(exception);
            }

            throw;
        }
        finally
        {
            _controlBatch.Clear();
        }
    }

    private async ValueTask ExecuteControlDirectAsync(
        MqttZeroEgressControlFrame frame,
        CancellationToken cancellationToken)
    {
        _controlBatch.Clear();
        _controlBatch.Add(frame);
        try
        {
            await RunControlBatchAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _controlBatch.Clear();
        }
    }

    private void EnqueueControl(MqttZeroEgressControlFrame frame)
    {
        ThrowIfStopping();
        if (_controlQueue.Count >= MaximumControlQueueLength)
        {
            InvalidOperationException exception = new(
                "The outbound control queue is full.");
            frame.Fail(exception);
            FailQueuedControls(exception);
            _faulted(exception);
            throw exception;
        }

        _controlQueue.Enqueue(frame);
        Schedule();
    }

    private void Schedule()
    {
        if (Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0)
        {
            if (!_shard.TryEnqueue(_work))
            {
                _ = ScheduleWorkAsync();
            }
        }
    }

    private async Task ScheduleWorkAsync()
    {
        try
        {
            await _shard.EnqueueAsync(
                _work,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            Volatile.Write(ref _scheduled, 0);
            FailQueuedControls(exception);
            _faulted(exception);
            _stopped.TrySetResult();
        }
    }

    private void FailQueuedControls(Exception exception)
    {
        while (_controlQueue.Count > 0)
        {
            MqttZeroEgressControlFrame frame = _controlQueue.Dequeue();
            frame.Fail(exception);
        }

        for (int index = 0; index < _controlBatch.Count; index++)
        {
            MqttZeroEgressControlFrame frame = _controlBatch[index];
            frame.Fail(exception);
        }

        _controlBatch.Clear();
    }

    private void ThrowIfStopping()
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _stopping) != 0,
            this);
    }
}
