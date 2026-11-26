using System.IO.Pipelines;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroSessionEgress : IAsyncDisposable
{
    private readonly MqttZeroEgressActor _actor;

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
}
