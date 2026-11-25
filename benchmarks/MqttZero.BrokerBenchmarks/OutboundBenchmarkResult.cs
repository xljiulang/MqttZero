namespace MqttZero.BrokerBenchmarks;

internal sealed record OutboundBenchmarkResult(
    long PublishedMessages,
    long ReceivedMessages,
    long FailedOperations,
    double ThroughputPerSecond,
    double WaitP50Microseconds,
    double WaitP99Microseconds,
    double PublishP50Microseconds,
    double PublishP99Microseconds,
    string? FirstError,
    BenchmarkFanoutDiagnostics Diagnostics);
