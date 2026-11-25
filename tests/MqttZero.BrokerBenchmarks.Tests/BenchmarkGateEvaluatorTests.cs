using MqttZero.BrokerBenchmarks;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkGateEvaluatorTests
{
    [Fact]
    public void GateRequiresStrictlyBetterThroughputAndP99()
    {
        BrokerComparisonResult[] results =
        [
            Create("mqttzero", throughput: 100, p99: 100, failures: 0),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.False(gate.Passed);
        Assert.False(Assert.Single(gate.Summaries).Passed);
        Assert.Contains("throughput", gate.Summaries[0].FailureReason);
        Assert.Contains("p99", gate.Summaries[0].FailureReason);
    }

    [Fact]
    public void GateRejectsAnyBrokerFailures()
    {
        BrokerComparisonResult[] results =
        [
            Create("mqttzero", throughput: 101, p99: 99, failures: 1),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.False(gate.Passed);
        Assert.Contains("failure", gate.Summaries[0].FailureReason);
    }

    [Fact]
    public void GatePassesOnlyWhenBothMetricsAreStrictlyBetter()
    {
        BrokerComparisonResult[] results =
        [
            Create("mqttzero", throughput: 101, p99: 99, failures: 0),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.True(gate.Passed);
        Assert.True(Assert.Single(gate.Summaries).Passed);
        Assert.Null(gate.Summaries[0].FailureReason);
    }

    [Fact]
    public void GateRejectsMissingBroker()
    {
        BrokerComparisonResult[] results =
        [
            Create("mqttzero", throughput: 101, p99: 99, failures: 0)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.False(gate.Passed);
        Assert.Contains("mqttnet", Assert.Single(gate.Summaries).FailureReason);
    }

    [Fact]
    public void GateRejectsMismatchedRepetitionCounts()
    {
        BrokerComparisonResult[] results =
        [
            Create("mqttzero", throughput: 101, p99: 99, failures: 0),
            Create("mqttzero", throughput: 102, p99: 98, failures: 0),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.False(gate.Passed);
        Assert.Contains("repetition", Assert.Single(gate.Summaries).FailureReason);
    }

    private static BrokerComparisonResult Create(
        string broker,
        double throughput,
        double p99,
        long failures)
    {
        return new BrokerComparisonResult
        {
            Broker = broker,
            Qos = 1,
            Subscribers = 1,
            PayloadSize = 32,
            Repetition = 1,
            BrokerObservation = new BenchmarkProcessObservation(
                0,
                0,
                0,
                0),
            Result = new BrokerBenchmarkResult
            {
                PublishedMessages = 1,
                ReceivedMessages = 1,
                FailedOperations = failures,
                WarmupPublishedMessages = 1,
                WarmupReceivedMessages = 1,
                ThroughputPerSecond = throughput,
                LatencyP50Microseconds = p99,
                LatencyP95Microseconds = p99,
                LatencyP99Microseconds = p99,
                LoadObservation = new BenchmarkLoadObservation(
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0)
            }
        };
    }
}
