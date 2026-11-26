using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;
using System.IO.Pipelines;
using System.Diagnostics;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroSessionEgress : IAsyncDisposable
{
    private readonly MqttZeroEgressActor _actor;
    private readonly MqttZeroBroker? _broker;
    private readonly MqttZeroSessionState? _session;
    private readonly MqttZeroClientConnection? _connection;
    private int _resumePending;

    internal MqttZeroSessionEgress(
        MqttZeroEgressScheduler scheduler,
        Func<
            IReadOnlyList<MqttZeroEgressControlFrame>,
            CancellationToken,
            ValueTask> writeControls,
        Func<CancellationToken, ValueTask<bool>> drainData,
        Action<Exception> faulted)
    {
        _actor = new MqttZeroEgressActor(
            scheduler,
            writeControls,
            drainData,
            faulted);
    }

    internal MqttZeroSessionEgress(
        MqttZeroBroker broker,
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroEgressScheduler scheduler,
        Action<Exception> faulted)
    {
        _broker = broker;
        _session = session;
        _connection = connection;
        _resumePending = 1;
        _actor = new MqttZeroEgressActor(
            scheduler,
            connection.WriteEncodedControlBatchAsync,
            DrainOwnedAsync,
            faulted);
    }

    internal MqttZeroSessionEgressState State
    {
        get
        {
            return (MqttZeroSessionEgressState)(int)_actor.State;
        }
    }

    internal bool IsStopping
    {
        get
        {
            return _actor.IsStopping;
        }
    }

    internal MqttZeroEgressActor Actor
    {
        get
        {
            return _actor;
        }
    }

    internal void Signal()
    {
        _actor.Signal();
    }

    internal bool BeginFlush(ValueTask<FlushResult> flush)
    {
        return _actor.BeginFlush(flush);
    }

    internal ValueTask<bool> PostControlAsync(
        MqttZeroEgressControlFrame frame,
        CancellationToken cancellationToken)
    {
        return _actor.PostControlAsync(frame, cancellationToken);
    }

    internal ValueTask StopAsync()
    {
        return _actor.StopAsync();
    }

    public ValueTask DisposeAsync()
    {
        return _actor.DisposeAsync();
    }

    private async ValueTask<bool> DrainOwnedAsync(
        CancellationToken cancellationToken)
    {
        MqttZeroBroker broker = _broker ??
            throw new InvalidOperationException(
                "The session egress owner is not attached to a broker.");
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
                await MqttZeroBroker.ResumeInflightMessagesAsync(
                    session,
                    connection,
                    cancellationToken).ConfigureAwait(false);
            }

            return await broker.DrainQueuedMessagesOnceAsync(
                session,
                connection,
                this,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            broker.RecordEgressDrainTiming(
                Stopwatch.GetTimestamp() - started);
        }
    }
}
