namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkSummary(
    int Qos,
    int Subscribers,
    int PayloadSize,
    double MqttZeroThroughput,
    double MqttNetThroughput,
    double ThroughputRatio,
    double MqttZeroP99Microseconds,
    double MqttNetP99Microseconds,
    double P99Ratio,
    long MqttZeroFailures,
    long MqttNetFailures,
    bool Passed,
    string? FailureReason);
