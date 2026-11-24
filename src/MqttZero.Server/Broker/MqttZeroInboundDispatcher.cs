using System.Threading.Channels;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroInboundDispatcher
{
    private readonly Channel<MqttZeroInboundWork>[] _channels;
    private readonly Func<MqttZeroInboundWork, CancellationToken, ValueTask> _route;
    private readonly Action<Exception> _faulted;
    private readonly Task[] _workers;
    private int _stopped;

    public MqttZeroInboundDispatcher(
        int workerCount,
        int queueCapacity,
        Func<MqttZeroInboundWork, CancellationToken, ValueTask> route,
        Action<Exception> faulted,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(workerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueCapacity);
        _route = route ?? throw new ArgumentNullException(nameof(route));
        _faulted = faulted ?? throw new ArgumentNullException(nameof(faulted));
        _channels = new Channel<MqttZeroInboundWork>[workerCount];
        _workers = new Task[workerCount];
        for (int index = 0; index < workerCount; index++)
        {
            _channels[index] = Channel.CreateBounded<MqttZeroInboundWork>(
                new BoundedChannelOptions(queueCapacity)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait,
                    AllowSynchronousContinuations = false
                });
        }

        for (int index = 0; index < workerCount; index++)
        {
            int shard = index;
            _workers[index] = RunAsync(shard, cancellationToken);
        }
    }

    public ValueTask EnqueueAsync(
        long connectionId,
        MqttZeroInboundWork work,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _stopped) != 0, this);
        ArgumentNullException.ThrowIfNull(work);
        int shard = (int)(connectionId % _channels.Length);
        return _channels[shard].Writer.WriteAsync(work, cancellationToken);
    }

    public async ValueTask StopAsync()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        for (int index = 0; index < _channels.Length; index++)
        {
            _channels[index].Writer.TryComplete();
        }

        await Task.WhenAll(_workers).ConfigureAwait(false);
    }

    private async Task RunAsync(
        int shard,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (MqttZeroInboundWork work in
                _channels[shard].Reader.ReadAllAsync(cancellationToken))
            {
                try
                {
                    await _route(work, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    _faulted(exception);
                }
                finally
                {
                    work.Release();
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
