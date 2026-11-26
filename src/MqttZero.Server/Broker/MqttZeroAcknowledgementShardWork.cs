using MqttZero.Memory;
using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroAcknowledgementShardWork : MqttZeroShardWork
{
    private static readonly MqttZeroBoundedObjectPool<
        MqttZeroAcknowledgementShardWork> Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroAcknowledgementShardWork>.
                DefaultCapacity);
    private MqttZeroAcknowledgementWork? _acknowledgementWork;
    private Action<Exception>? _faulted;
    private Func<
        MqttZeroAcknowledgementWork,
        CancellationToken,
        ValueTask>? _process;

    private MqttZeroAcknowledgementShardWork()
    {
    }

    public static MqttZeroAcknowledgementShardWork Rent(
        MqttZeroAcknowledgementWork work,
        Func<
            MqttZeroAcknowledgementWork,
            CancellationToken,
            ValueTask> process,
        Action<Exception> faulted)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(process);
        ArgumentNullException.ThrowIfNull(faulted);
        if (!Pool.TryRent(
                out MqttZeroAcknowledgementShardWork? shardWork) ||
            shardWork is null)
        {
            shardWork = new MqttZeroAcknowledgementShardWork();
        }

        shardWork._acknowledgementWork = work;
        shardWork._process = process;
        shardWork._faulted = faulted;
        return shardWork;
    }

    public override async ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        MqttZeroAcknowledgementWork work =
            _acknowledgementWork ??
                throw new InvalidOperationException(
                    "The acknowledgement work was not initialized.");
        Func<
            MqttZeroAcknowledgementWork,
            CancellationToken,
            ValueTask> process =
            _process ??
                throw new InvalidOperationException(
                    "The acknowledgement processor was not initialized.");
        try
        {
            await process(work, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _faulted?.Invoke(exception);
            throw;
        }
    }

    public override void Release()
    {
        _acknowledgementWork?.Release();
        _acknowledgementWork = null;
        _process = null;
        _faulted = null;
        _ = Pool.Return(this);
    }
}
