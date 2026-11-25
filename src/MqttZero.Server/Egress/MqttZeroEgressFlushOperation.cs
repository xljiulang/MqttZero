using System.IO.Pipelines;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressFlushOperation
{
    private readonly Action<Exception?> _completed;
    private readonly ValueTask<FlushResult> _flush;

    public MqttZeroEgressFlushOperation(
        ValueTask<FlushResult> flush,
        Action<Exception?> completed)
    {
        _flush = flush;
        _completed = completed;
    }

    public async Task RunAsync()
    {
        Exception? failure = null;
        try
        {
            FlushResult result = await _flush.ConfigureAwait(false);
            if (result.IsCompleted)
            {
                failure = new IOException(
                    "The MQTT output writer completed during flush.");
            }
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        _completed(failure);
    }
}
