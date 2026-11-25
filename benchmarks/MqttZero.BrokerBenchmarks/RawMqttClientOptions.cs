namespace MqttZero.BrokerBenchmarks;

internal sealed class RawMqttClientOptions
{
    public string Host { get; set; } = "127.0.0.1";

    public int Port { get; set; }

    public string? ClientId { get; set; }

    public ushort KeepAliveSeconds { get; set; }
}
