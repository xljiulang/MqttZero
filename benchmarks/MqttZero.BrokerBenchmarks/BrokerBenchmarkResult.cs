namespace MqttZero.BrokerBenchmarks;

internal sealed class BrokerBenchmarkResult
{
    public long PublishedMessages { get; init; }

    public int Publishers { get; init; } = 1;

    public long ReceivedMessages { get; init; }

    public long FailedOperations { get; init; }

    public long WarmupPublishedMessages { get; init; }

    public long WarmupReceivedMessages { get; init; }

    public long MeasurementReceivedMessages { get; init; }

    public double PublishWindowThroughputPerSecond { get; init; }

    public double EndToEndThroughputPerSecond { get; init; }

    public double PublishCallLatencyP50Microseconds { get; init; }

    public double PublishCallLatencyP95Microseconds { get; init; }

    public double PublishCallLatencyP99Microseconds { get; init; }

    public double PublishCallLatencyMaxMicroseconds { get; init; }

    public double DeliveryLatencyP50Microseconds { get; init; }

    public double DeliveryLatencyP95Microseconds { get; init; }

    public double DeliveryLatencyP99Microseconds { get; init; }

    public double DeliveryLatencyMaxMicroseconds { get; init; }

    public long SequenceRegressions { get; init; }

    public long DuplicateDeliveryMessages { get; init; }

    public long MissingDeliveryMessages { get; init; }

    public long UnsequencedDeliveryMessages { get; init; }

    public required BenchmarkLoadObservation LoadObservation { get; init; }

    public double ConnectionSetupMilliseconds { get; init; }

    public double WarmupMilliseconds { get; init; }

    public double MeasurementMilliseconds { get; init; }

    public double WarmupDrainMilliseconds { get; init; }

    public double MeasurementDrainMilliseconds { get; init; }

    public double DrainRatio { get; init; }

    public string? FirstError { get; init; }
}
