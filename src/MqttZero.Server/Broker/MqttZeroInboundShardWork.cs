using MqttZero.Server.Scheduling;
using System.Collections.Concurrent;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroInboundShardWork : MqttZeroShardWork
{
    private static readonly ConcurrentQueue<MqttZeroInboundShardWork> Pool =
        new();
    private Action<Exception>? _faulted;
    private Func<MqttZeroInboundWork, CancellationToken, ValueTask>? _route;
    private MqttZeroInboundWork? _work;

    private MqttZeroInboundShardWork()
    {
    }

    public static MqttZeroInboundShardWork Rent(
        MqttZeroInboundWork work,
        Func<MqttZeroInboundWork, CancellationToken, ValueTask> route,
        Action<Exception> faulted)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(route);
        ArgumentNullException.ThrowIfNull(faulted);
        if (!Pool.TryDequeue(out MqttZeroInboundShardWork? shardWork))
        {
            shardWork = new MqttZeroInboundShardWork();
        }

        shardWork._work = work;
        shardWork._route = route;
        shardWork._faulted = faulted;
        return shardWork;
    }

    public override async ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        MqttZeroInboundWork work = _work ??
            throw new InvalidOperationException(
                "The inbound shard work was not initialized.");
        Func<MqttZeroInboundWork, CancellationToken, ValueTask> route =
            _route ??
                throw new InvalidOperationException(
                    "The inbound shard route was not initialized.");
        try
        {
            await route(work, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _faulted?.Invoke(exception);
            throw;
        }
    }

    public override void Release()
    {
        _work?.Release();
        _work = null;
        _route = null;
        _faulted = null;
        Pool.Enqueue(this);
    }
}
