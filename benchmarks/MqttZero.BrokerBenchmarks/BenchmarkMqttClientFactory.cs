namespace MqttZero.BrokerBenchmarks;

internal sealed class BenchmarkMqttClientFactory :
    IBenchmarkMqttClientFactory
{
    private readonly int _port;
    private readonly Func<BenchmarkDeliveryTracker> _trackerFactory;
    private readonly TimeSpan _receiveDelay;

    public BenchmarkMqttClientFactory(
        int port,
        Func<BenchmarkDeliveryTracker>? trackerFactory = null,
        TimeSpan receiveDelay = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(port);
        _port = port;
        _receiveDelay = receiveDelay;
        _trackerFactory = trackerFactory ??
            (() => new BenchmarkDeliveryTracker());
    }

    public IBenchmarkMqttClient Create()
    {
        BenchmarkDeliveryTracker tracker = _trackerFactory();
        return new RawMqttClient(
            new RawMqttClientOptions
            {
                Host = "127.0.0.1",
                Port = _port
            },
            tracker,
            _receiveDelay);
    }
}
