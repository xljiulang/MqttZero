namespace MqttZero.BrokerBenchmarks;

internal sealed class BrokerAbRoundResult
{
    public int Round { get; init; }

    public required string Variant { get; init; }

    public string? Commit { get; init; }

    public required BrokerBenchmarkResult Result { get; init; }

    public required BenchmarkProcessObservation BrokerObservation { get; init; }

    public required BenchmarkProcessObservation ClientObservation { get; init; }

    public BrokerControlProbeSnapshot? ControlProbe { get; init; }
}
