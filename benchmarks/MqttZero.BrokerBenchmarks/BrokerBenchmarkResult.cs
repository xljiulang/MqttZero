namespace MqttZero.BrokerBenchmarks;

internal sealed class BrokerBenchmarkResult
{
    public long PublishedMessages { get; init; }

    public long ReceivedMessages { get; init; }

    public long FailedOperations { get; init; }

    public double ThroughputPerSecond { get; init; }

    public double LatencyP50Microseconds { get; init; }

    public double LatencyP95Microseconds { get; init; }

    public double LatencyP99Microseconds { get; init; }

    public string? FirstError { get; init; }
}
