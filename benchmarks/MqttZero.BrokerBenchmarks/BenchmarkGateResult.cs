namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkGateResult(
    IReadOnlyList<BenchmarkSummary> Summaries,
    bool Passed,
    IReadOnlyList<string> Errors);
