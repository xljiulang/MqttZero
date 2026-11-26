using MqttZero.Server.Broker;
using MqttZero.Server.Scheduling;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressScheduler : IAsyncDisposable
{
    private const int MaximumActorExecutionsPerPass = 64;
    private static readonly long PassBudgetTimestampTicks =
        Math.Max(1, Stopwatch.Frequency / 4000);

    private readonly MqttZeroFanoutDiagnostics? _diagnostics;
    private readonly ConcurrentQueue<MqttZeroSessionEgress> _ready = new();
    private readonly TaskCompletionSource _stopped = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _signalsDrained = new(
        TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly MqttZeroEgressSchedulerWork _work;
    private int _activeSignals;
    private int _scheduled;
    private int _stopping;

    public MqttZeroEgressScheduler(
        MqttZeroEventLoop shard,
        MqttZeroFanoutDiagnostics? diagnostics)
    {
        Shard = shard ?? throw new ArgumentNullException(nameof(shard));
        _diagnostics = diagnostics;
        _work = new MqttZeroEgressSchedulerWork(this);
    }

    public MqttZeroEventLoop Shard { get; }

    public void Signal(MqttZeroSessionEgress egress)
    {
        ArgumentNullException.ThrowIfNull(egress);
        Interlocked.Increment(ref _activeSignals);
        try
        {
            if (Volatile.Read(ref _stopping) != 0 ||
                !egress.TryMarkReady())
            {
                return;
            }

            _diagnostics?.RecordEgressScheduledActor();
            _ready.Enqueue(egress);
            Schedule();
        }
        finally
        {
            if (Interlocked.Decrement(ref _activeSignals) == 0 &&
                Volatile.Read(ref _stopping) != 0)
            {
                _signalsDrained.TrySetResult();
            }
        }
    }

    internal ValueTask SignalAndDrainCurrentShardAsync(
        MqttZeroSessionEgress egress)
    {
        ArgumentNullException.ThrowIfNull(egress);
        if (!Shard.IsCurrent)
        {
            throw new InvalidOperationException(
                "The egress scheduler must run on its owning shard.");
        }

        Signal(egress);
        return ExecuteAsync(CancellationToken.None);
    }

    public async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
        {
            await _stopped.Task.ConfigureAwait(false);
            return;
        }

        if (Volatile.Read(ref _activeSignals) == 0)
        {
            _signalsDrained.TrySetResult();
        }

        await _signalsDrained.Task.ConfigureAwait(false);
        if (_ready.IsEmpty)
        {
            _stopped.TrySetResult();
            return;
        }

        Schedule();
        await _stopped.Task.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    internal async ValueTask ExecuteAsync(
        CancellationToken cancellationToken)
    {
        long started = Stopwatch.GetTimestamp();
        int actorCount = 0;
        try
        {
            while (actorCount < MaximumActorExecutionsPerPass &&
                Stopwatch.GetElapsedTime(started) <
                    TimeSpan.FromTicks(PassBudgetTimestampTicks) &&
                _ready.TryDequeue(out MqttZeroSessionEgress? egress))
            {
                egress.MarkExecuting();
                _diagnostics?.RecordEgressDrain();
                try
                {
                    await egress.ExecuteAsync(cancellationToken)
                        .ConfigureAwait(false);
                }
                finally
                {
                    egress.OnExecutionReleased();
                }

                actorCount++;
            }
        }
        finally
        {
            _diagnostics?.RecordEgressSchedulerPass(
                actorCount,
                Stopwatch.GetTimestamp() - started);
        }
    }

    internal void OnWorkReleased()
    {
        Volatile.Write(ref _scheduled, 0);
        Thread.MemoryBarrier();
        if (!_ready.IsEmpty)
        {
            Schedule();
            return;
        }

        if (Volatile.Read(ref _stopping) != 0 &&
            Volatile.Read(ref _activeSignals) == 0)
        {
            _stopped.TrySetResult();
        }
    }

    private void Schedule()
    {
        if (Interlocked.CompareExchange(ref _scheduled, 1, 0) == 0 &&
            !Shard.TryEnqueue(_work))
        {
            _ = ScheduleWorkAsync();
        }
    }

    private async Task ScheduleWorkAsync()
    {
        try
        {
            await Shard.EnqueueAsync(
                _work,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception)
        {
            Volatile.Write(ref _scheduled, 0);
            _stopped.TrySetResult();
        }
    }
}
