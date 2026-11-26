using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Observers;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Diagnostics;
using System.IO.Pipelines;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroSessionEgress : IAsyncDisposable
{
    private const int MaximumControlQueueLength = 1024;

    private readonly Queue<MqttZeroEgressControlFrame> _controlQueue = new();
    private readonly List<MqttZeroEgressControlFrame> _controlBatch = [];
    private readonly MqttZeroClientConnection? _connection;
    private readonly Func<CancellationToken, ValueTask<bool>> _drainData;
    private readonly Action<Exception> _faulted;
    private readonly MqttZeroEgressFlushOperation _flushOperation;
    private readonly MqttZeroEgressScheduler _scheduler;
    private readonly MqttZeroSessionState? _session;
    private readonly MqttZeroSessionEgressServices? _services;
    private readonly MqttZeroSessionEgressBufferState _stateData = new();
    private readonly TaskCompletionSource _stopped = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Func<
        IReadOnlyList<MqttZeroEgressControlFrame>,
        CancellationToken,
        ValueTask> _writeControls;
    private int _controlSignaled;
    private int _dataSignaled;
    private int _executing;
    private Exception? _flushFailure;
    private int _flushPending;
    private TaskCompletionSource? _flushStopWaiter;
    private int _ready;
    private int _resumePending;
    private int _state = (int)MqttZeroSessionEgressState.Idle;
    private int _stopping;

    internal MqttZeroSessionEgress(
        MqttZeroEgressScheduler scheduler,
        Func<
            IReadOnlyList<MqttZeroEgressControlFrame>,
            CancellationToken,
            ValueTask> writeControls,
        Func<CancellationToken, ValueTask<bool>> drainData,
        Action<Exception> faulted)
    {
        _scheduler = scheduler ??
            throw new ArgumentNullException(nameof(scheduler));
        _writeControls = writeControls ??
            throw new ArgumentNullException(nameof(writeControls));
        _drainData = drainData ??
            throw new ArgumentNullException(nameof(drainData));
        _faulted = faulted ?? throw new ArgumentNullException(nameof(faulted));
        _flushOperation = new MqttZeroEgressFlushOperation(
            OnFlushCompleted);
    }

    internal MqttZeroSessionEgress(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroEgressScheduler scheduler,
        MqttZeroSessionEgressServices services,
        Action<Exception> faulted)
    {
        _session = session ??
            throw new ArgumentNullException(nameof(session));
        _connection = connection ??
            throw new ArgumentNullException(nameof(connection));
        _scheduler = scheduler ??
            throw new ArgumentNullException(nameof(scheduler));
        _services = services ??
            throw new ArgumentNullException(nameof(services));
        _faulted = faulted ?? throw new ArgumentNullException(nameof(faulted));
        _resumePending = 1;
        _writeControls = connection.WriteEncodedControlBatchAsync;
        _drainData = DrainOwnedAsync;
        _flushOperation = new MqttZeroEgressFlushOperation(
            OnFlushCompleted);
    }

    internal MqttZeroSessionEgressState State
    {
        get
        {
            return (MqttZeroSessionEgressState)Volatile.Read(
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

    internal MqttZeroSessionEgressBufferState StateData
    {
        get
        {
            return _stateData;
        }
    }

    internal void Signal()
    {
        if (IsStopping)
        {
            return;
        }

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
            (int)MqttZeroSessionEgressState.FlushPending);
        _flushOperation.Start(flush);
        return false;
    }

    internal async ValueTask<bool> PostControlAsync(
        MqttZeroEgressControlFrame frame,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(frame);
        ThrowIfStopping();
        cancellationToken.ThrowIfCancellationRequested();
        if (_scheduler.Shard.IsCurrent &&
            Volatile.Read(ref _flushPending) == 0)
        {
            await ExecuteControlDirectAsync(
                frame,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (_scheduler.Shard.IsCurrent)
        {
            frame.MarkForActorRelease();
            EnqueueControl(frame);
            return true;
        }

        await _scheduler.Shard.InvokeAsync(
            _ =>
            {
                EnqueueControl(frame);
                return ValueTask.FromResult(true);
            },
            CancellationToken.None).ConfigureAwait(false);
        await frame.WaitAsync().ConfigureAwait(false);
        return false;
    }

    internal async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) == 0)
        {
            bool ownsShard = _scheduler.Shard.IsCurrent;
            Volatile.Write(
                ref _state,
                (int)MqttZeroSessionEgressState.Stopping);
            if (ownsShard &&
                Volatile.Read(ref _flushPending) != 0)
            {
                await WaitForPendingFlushAsync().ConfigureAwait(false);
            }

            if (ownsShard)
            {
                await _scheduler
                    .SignalAndDrainCurrentShardAsync(this)
                    .ConfigureAwait(false);
            }
            else
            {
                _scheduler.Signal(this);
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
        Exception? flushFailure = Interlocked.Exchange(
            ref _flushFailure,
            null);
        if (flushFailure != null)
        {
            Volatile.Write(ref _stopping, 1);
            Volatile.Write(
                ref _state,
                (int)MqttZeroSessionEgressState.Stopping);
            FailQueuedControls(flushFailure);
            _faulted(flushFailure);
            TryCompleteStop();
            return;
        }

        if (Volatile.Read(ref _stopping) != 0)
        {
            FailQueuedControls(
                new ObjectDisposedException(nameof(MqttZeroSessionEgress)));
            TryCompleteStop();
            return;
        }

        if (Volatile.Read(ref _flushPending) != 0)
        {
            return;
        }

        Volatile.Write(
            ref _state,
            (int)MqttZeroSessionEgressState.Draining);
        Volatile.Write(ref _controlSignaled, 0);
        bool dataSignaled = Interlocked.Exchange(
            ref _dataSignaled,
            0) != 0;
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

            if (Volatile.Read(ref _stopping) == 0 &&
                (dataSignaled ||
                    Volatile.Read(ref _dataSignaled) != 0))
            {
                bool hasMoreData = await _drainData(cancellationToken)
                    .ConfigureAwait(false);
                if (hasMoreData)
                {
                    Volatile.Write(ref _dataSignaled, 1);
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
                    ? (int)MqttZeroSessionEgressState.Stopping
                    : Volatile.Read(ref _flushPending) != 0
                        ? (int)MqttZeroSessionEgressState.FlushPending
                        : (int)MqttZeroSessionEgressState.Idle);
        }
    }

    internal bool TryMarkReady()
    {
        return Interlocked.CompareExchange(ref _ready, 1, 0) == 0;
    }

    internal void MarkExecuting()
    {
        Volatile.Write(ref _executing, 1);
        Volatile.Write(ref _ready, 0);
        Thread.MemoryBarrier();
    }

    internal void OnExecutionReleased()
    {
        Volatile.Write(ref _executing, 0);
        Thread.MemoryBarrier();
        if (Volatile.Read(ref _stopping) != 0)
        {
            TryCompleteStop();
            return;
        }

        if (Volatile.Read(ref _flushPending) == 0 &&
            (Volatile.Read(ref _controlSignaled) != 0 ||
                _controlQueue.Count > 0 ||
                Volatile.Read(ref _dataSignaled) != 0))
        {
            Schedule();
        }
    }

    private async ValueTask<bool> DrainOwnedAsync(
        CancellationToken cancellationToken)
    {
        MqttZeroSessionEgressServices services = _services ??
            throw new InvalidOperationException(
                "The session egress owner is not attached to broker services.");
        MqttZeroSessionState session = _session ??
            throw new InvalidOperationException(
                "The session egress owner has no session.");
        MqttZeroClientConnection connection = _connection ??
            throw new InvalidOperationException(
                "The session egress owner has no connection.");

        long started = Stopwatch.GetTimestamp();
        try
        {
            if (Interlocked.Exchange(ref _resumePending, 0) != 0)
            {
                await ResumeInflightMessagesAsync(
                    session,
                    connection,
                    cancellationToken).ConfigureAwait(false);
            }

            return await DrainQueuedMessagesAsync(
                session,
                connection,
                services,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            services.Diagnostics.RecordEgressDrainTiming(
                Stopwatch.GetTimestamp() - started);
        }
    }

    private static async ValueTask ResumeInflightMessagesAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        CancellationToken cancellationToken)
    {
        List<MqttZeroQueuedMessage> inflight =
            [.. session.InflightMessages.Values];

        foreach (MqttZeroQueuedMessage message in inflight)
        {
            if (message.State == MqttZeroOutboundState.AwaitPubComp)
            {
                await connection.SendPubRelAsync(
                    message.PacketIdentifier,
                    MqttReasonCode.Success,
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await connection.SendPublishAsync(
                    message,
                    true,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<bool> DrainQueuedMessagesAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroSessionEgressServices services,
        CancellationToken cancellationToken)
    {
        MqttZeroShardOwnership.EnsureCurrent(
            session.ShardId,
            nameof(DrainQueuedMessagesAsync),
            services.Diagnostics);
        while (true)
        {
            long batchStartedTimestamp =
                services.TimeProvider.GetTimestamp();
            long batchDurationTicks = Math.Max(
                1,
                (long)(
                    services.Options.OutboundBatchDuration.TotalSeconds *
                    services.TimeProvider.TimestampFrequency));
            MqttZeroEgressBatchBudget budget = new(
                services.Options.OutboundBatchBytes,
                services.Options.OutboundBatchMessages,
                batchStartedTimestamp + batchDurationTicks);
            List<MqttZeroQueuedMessage> expired =
                _stateData.ExpiredMessages;
            List<MqttZeroQueuedMessage> batch =
                _stateData.OutboundBatch;
            bool diagnosticsEnabled = services.Diagnostics.Enabled;
            _stateData.SelectBatch(
                session,
                connection,
                services.Options,
                services.TimeProvider,
                services.Diagnostics,
                ref budget,
                out MqttZeroQueuedMessage? next,
                out bool hasNext,
                out bool nextTooLarge,
                out _);
            services.RescheduleMessageExpiry(session);

            for (int index = 0; index < expired.Count; index++)
            {
                MqttZeroQueuedMessage expiredMessage = expired[index];
                try
                {
                    await services.DeleteMessageAsync(
                        session.ClientId,
                        expiredMessage.MessageId,
                        cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    for (int restore = expired.Count - 1;
                        restore >= index;
                        restore--)
                    {
                        session.PendingMessages.Prepend(
                            expired[restore]);
                    }

                    throw;
                }

                await services.NotifyDroppedAsync(
                    session,
                    expiredMessage.Lease,
                    MqttZeroMessageDropReason.Expired,
                    cancellationToken).ConfigureAwait(false);
                expiredMessage.Release();
            }

            expired.Clear();
            if (nextTooLarge && next is { } tooLarge)
            {
                try
                {
                    await services.DeleteMessageAsync(
                        session.ClientId,
                        tooLarge.MessageId,
                        cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    session.PendingMessages.Prepend(tooLarge);

                    throw;
                }

                await services.NotifyDroppedAsync(
                    session,
                    tooLarge.Lease,
                    MqttZeroMessageDropReason.PacketTooLarge,
                    cancellationToken).ConfigureAwait(false);
                tooLarge.Release();
                continue;
            }

            if (!hasNext)
            {
                services.RescheduleMessageExpiry(session);
                return false;
            }

            if (batch.Count > 0)
            {
                for (int index = 0; index < batch.Count; index++)
                {
                    MqttZeroQueuedMessage message = batch[index];
                    if (message.QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.AtMostOnce)
                    {
                        continue;
                    }

                    try
                    {
                        await services.TransitionQueuedToOutboundAsync(
                            message,
                            session.ClientId,
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        for (int restore = batch.Count - 1;
                            restore >= index;
                            restore--)
                        {
                            MqttZeroQueuedMessage restoreMessage =
                                batch[restore];
                            if (restoreMessage.QualityOfServiceLevel !=
                                MqttQualityOfServiceLevel.AtMostOnce)
                            {
                                session.InflightMessages.Remove(
                                    restoreMessage.PacketIdentifier);
                            }

                            session.PendingMessages.Prepend(
                                restoreMessage);
                        }

                        batch.Clear();
                        throw;
                    }
                }

                bool qosZeroMessagesReleased = false;
                try
                {
                    services.Diagnostics.RecordEgressDataBatch(
                        batch.Count,
                        budget.EncodedBytes);
                    services.Diagnostics.RecordEgressFlushStart();
                    long encodeStarted = diagnosticsEnabled
                        ? Stopwatch.GetTimestamp()
                        : 0;
                    int encodedBytes =
                        await connection.EncodePublishBatchAsync(
                            batch,
                            cancellationToken).ConfigureAwait(false);
                    bool encoded = encodedBytes > 0;
                    if (diagnosticsEnabled)
                    {
                        services.Diagnostics.RecordPublishEncode(
                            Stopwatch.GetTimestamp() - encodeStarted);
                    }

                    if (!encoded)
                    {
                        long writeStarted = diagnosticsEnabled
                            ? Stopwatch.GetTimestamp()
                            : 0;
                        await connection.SendPublishBatchAsync(
                            batch,
                            cancellationToken).ConfigureAwait(false);
                        if (diagnosticsEnabled)
                        {
                            services.Diagnostics.RecordPublishWrite(
                                Stopwatch.GetTimestamp() - writeStarted);
                        }
                    }

                    services.Diagnostics.RecordEncodedPublishes(
                        batch.Count);
                    bool flushPending = false;
                    if (connection.AllowPacketFragmentation)
                    {
                        flushPending = !BeginFlush(
                            connection.FlushOutputAsync(
                                cancellationToken));
                    }

                    MqttZeroSessionEgressBufferState.ReleaseQosZeroMessages(
                        batch);
                    qosZeroMessagesReleased = true;
                    if (flushPending)
                    {
                        services.Diagnostics.RecordFlushWait();
                        batch.Clear();
                        services.RescheduleMessageExpiry(session);
                        return session.PendingMessages.Count > 0;
                    }
                }
                catch
                {
                    if (!qosZeroMessagesReleased)
                    {
                        MqttZeroSessionEgressBufferState.ReleaseQosZeroMessages(
                            batch);
                    }

                    batch.Clear();
                    throw;
                }

                batch.Clear();
                services.RescheduleMessageExpiry(session);
                return session.PendingMessages.Count > 0;
            }
        }
    }

    private void OnFlushCompleted(Exception? exception)
    {
        Volatile.Write(ref _flushPending, 0);
        if (exception != null)
        {
            Volatile.Write(ref _flushFailure, exception);
        }

        TaskCompletionSource? flushStopWaiter = Interlocked.Exchange(
            ref _flushStopWaiter,
            null);
        flushStopWaiter?.TrySetResult();
        if (Volatile.Read(ref _stopping) != 0 ||
            Volatile.Read(ref _flushFailure) != null ||
            Volatile.Read(ref _controlSignaled) != 0 ||
            Volatile.Read(ref _dataSignaled) != 0)
        {
            _scheduler.Signal(this);
            return;
        }

        Volatile.Write(
            ref _state,
            (int)MqttZeroSessionEgressState.Idle);
    }

    private async ValueTask WaitForPendingFlushAsync()
    {
        TaskCompletionSource waiter = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        if (Interlocked.CompareExchange(
                ref _flushStopWaiter,
                waiter,
                null) != null)
        {
            throw new InvalidOperationException(
                "The session egress owner already has a flush stop waiter.");
        }

        if (Volatile.Read(ref _flushPending) == 0)
        {
            Interlocked.CompareExchange(
                ref _flushStopWaiter,
                null,
                waiter);
            return;
        }

        await waiter.Task.ConfigureAwait(false);
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
                frame.ReleaseIfActorOwned();
            }
        }
        catch (Exception exception)
        {
            for (int index = 0; index < _controlBatch.Count; index++)
            {
                MqttZeroEgressControlFrame frame = _controlBatch[index];
                frame.Fail(exception);
                frame.ReleaseIfActorOwned();
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
        Volatile.Write(ref _controlSignaled, 1);
        Schedule();
    }

    private void Schedule()
    {
        if (IsStopping)
        {
            TryCompleteStop();
            return;
        }

        _scheduler.Signal(this);
    }

    private void FailQueuedControls(Exception exception)
    {
        while (_controlQueue.Count > 0)
        {
            MqttZeroEgressControlFrame frame = _controlQueue.Dequeue();
            frame.Fail(exception);
            frame.ReleaseIfActorOwned();
        }

        for (int index = 0; index < _controlBatch.Count; index++)
        {
            MqttZeroEgressControlFrame frame = _controlBatch[index];
            frame.Fail(exception);
            frame.ReleaseIfActorOwned();
        }

        _controlBatch.Clear();
    }

    private void ThrowIfStopping()
    {
        ObjectDisposedException.ThrowIf(
            IsStopping,
            this);
    }

    private bool TryCompleteStop()
    {
        if (!IsStopping ||
            Volatile.Read(ref _executing) != 0 ||
            Volatile.Read(ref _ready) != 0 ||
            Volatile.Read(ref _flushPending) != 0 ||
            _controlQueue.Count != 0)
        {
            return false;
        }

        Volatile.Write(
            ref _state,
            (int)MqttZeroSessionEgressState.Stopped);
        _stopped.TrySetResult();
        return true;
    }
}
