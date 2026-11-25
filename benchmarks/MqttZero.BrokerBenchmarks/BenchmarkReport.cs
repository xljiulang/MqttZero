namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkReport(
    BenchmarkEnvironmentInfo Environment,
    IReadOnlyList<BrokerComparisonResult> Results,
    BenchmarkGateResult Gate);
