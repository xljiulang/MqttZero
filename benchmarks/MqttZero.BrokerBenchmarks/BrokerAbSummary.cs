namespace MqttZero.BrokerBenchmarks;

internal sealed record BrokerAbSummary(
    double BaselineEndToEndThroughput,
    double CandidateEndToEndThroughput,
    double ThroughputChangeRatio,
    double BaselinePublishCallP99Microseconds,
    double CandidatePublishCallP99Microseconds,
    double BaselineDeliveryP99Microseconds,
    double CandidateDeliveryP99Microseconds,
    double DeliveryP99Ratio,
    long Failures,
    long SequenceRegressions,
    long MissingDeliveryMessages,
    long DuplicateDeliveryMessages,
    string ThroughputMetric,
    double DrainRatio,
    bool Passed,
    string? FailureReason);
