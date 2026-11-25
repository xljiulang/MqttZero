namespace MqttZero.BrokerBenchmarks;

internal sealed class BenchmarkMqttClientFactory :
    IBenchmarkMqttClientFactory
{
    private readonly MqttNetBenchmarkMqttClientFactory _mqttNetFactory;
    private readonly int _port;
    private readonly BenchmarkClientKind _clientKind;

    public BenchmarkMqttClientFactory(
        BenchmarkClientKind clientKind,
        int port)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(port);
        _clientKind = clientKind;
        _port = port;
        _mqttNetFactory = new MqttNetBenchmarkMqttClientFactory(port);
    }

    public IBenchmarkMqttClient Create()
    {
        return _clientKind switch
        {
            BenchmarkClientKind.MqttNet => _mqttNetFactory.Create(),
            BenchmarkClientKind.Raw => new RawMqttClient(
                new RawMqttClientOptions
                {
                    Host = "127.0.0.1",
                    Port = _port
                }),
            _ => throw new ArgumentOutOfRangeException(
                nameof(_clientKind))
        };
    }
}
