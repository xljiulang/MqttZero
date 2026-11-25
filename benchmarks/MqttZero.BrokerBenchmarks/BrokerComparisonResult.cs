namespace MqttZero.BrokerBenchmarks;

internal sealed class BrokerComparisonResult
{
    public required string Broker { get; init; }

    public int Qos { get; init; }

    public int Subscribers { get; init; }

    public int PayloadSize { get; init; }

    public int Repetition { get; init; }

    public required BenchmarkProcessObservation BrokerObservation { get; init; }

    public required BrokerBenchmarkResult Result { get; init; }
}
