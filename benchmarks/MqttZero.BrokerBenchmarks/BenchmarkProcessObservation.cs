namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkProcessObservation(
    double TotalProcessorMilliseconds,
    long PrivateMemoryBytes,
    long PeakWorkingSetBytes,
    double UptimeMilliseconds);
