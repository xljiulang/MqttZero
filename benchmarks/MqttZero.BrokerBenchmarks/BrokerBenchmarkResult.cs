namespace MqttZero.BrokerBenchmarks;

internal sealed class BrokerBenchmarkResult
{
    public long PublishedMessages { get; init; }

    public long ReceivedMessages { get; init; }

    public long FailedOperations { get; init; }

    public long WarmupPublishedMessages { get; init; }

    public long WarmupReceivedMessages { get; init; }

    public double ThroughputPerSecond { get; init; }

    public double LatencyP50Microseconds { get; init; }

    public double LatencyP95Microseconds { get; init; }

    public double LatencyP99Microseconds { get; init; }

    public required BenchmarkLoadObservation LoadObservation { get; init; }

    public double ConnectionSetupMilliseconds { get; init; }

    public double WarmupMilliseconds { get; init; }

    public double MeasurementMilliseconds { get; init; }

    public double DrainMilliseconds { get; init; }

    public string? FirstError { get; init; }
}
