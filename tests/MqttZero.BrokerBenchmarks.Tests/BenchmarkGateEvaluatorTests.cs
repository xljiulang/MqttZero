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
    public void RankingComparisonCanBeDisabledForSmokeRuns()
    {
        BrokerComparisonResult[] results =
        [
            Create("mqttzero", throughput: 80, p99: 120, failures: 0),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];

        BenchmarkGateResult enforced = BenchmarkGateEvaluator.Evaluate(results);
        BenchmarkGateResult structuralOnly = BenchmarkGateEvaluator.Evaluate(
            results,
            enforceRankingComparison: false);

        Assert.False(enforced.Passed);
        Assert.Contains("throughput", enforced.Summaries[0].FailureReason);
        Assert.Contains("p99", enforced.Summaries[0].FailureReason);
        Assert.True(structuralOnly.Passed);
        BenchmarkSummary summary = Assert.Single(structuralOnly.Summaries);
        Assert.Null(summary.FailureReason);
        Assert.Equal(0.8, summary.ThroughputRatio, 3);
        Assert.Equal(1.2, summary.P99Ratio, 3);
    }

    [Fact]
    public void StructuralChecksStillApplyWhenRankingIsDisabled()
    {
        BrokerComparisonResult[] failedOperations =
        [
            Create("mqttzero", throughput: 80, p99: 120, failures: 2),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];
        BrokerComparisonResult[] deliveryGaps =
        [
            Create(
                "mqttzero",
                throughput: 80,
                p99: 120,
                failures: 0,
                missingDeliveries: 1),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];
        BrokerComparisonResult[] missingBroker =
        [
            Create("mqttzero", throughput: 80, p99: 120, failures: 0)
        ];

        Assert.False(
            BenchmarkGateEvaluator.Evaluate(
                failedOperations,
                enforceRankingComparison: false).Passed);
        Assert.False(
            BenchmarkGateEvaluator.Evaluate(
                deliveryGaps,
                enforceRankingComparison: false).Passed);
        Assert.False(
            BenchmarkGateEvaluator.Evaluate(
                missingBroker,
                enforceRankingComparison: false).Passed);
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

    [Fact]
    public void GateRejectsIncompleteMeasurementDrain()
    {
        BrokerComparisonResult[] results =
        [
            Create(
                "mqttzero",
                throughput: 101,
                p99: 99,
                failures: 0,
                publishedMessages: 10,
                subscribers: 10,
                receivedMessages: 99),
            Create(
                "mqttnet",
                throughput: 100,
                p99: 100,
                failures: 0,
                publishedMessages: 10,
                subscribers: 10,
                receivedMessages: 100)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.False(gate.Passed);
        Assert.Contains(
            "received",
            Assert.Single(gate.Summaries).FailureReason);
    }

    [Fact]
    public void GateUsesEndToEndThroughputInsteadOfPublishWindowThroughput()
    {
        BrokerComparisonResult[] results =
        [
            Create(
                "mqttzero",
                throughput: 200,
                p99: 99,
                failures: 0,
                publishedMessages: 50,
                endToEndThroughput: 50,
                measurementMilliseconds: 1000,
                drainMilliseconds: 1000),
            Create(
                "mqttnet",
                throughput: 50,
                p99: 100,
                failures: 0,
                publishedMessages: 50,
                endToEndThroughput: 50,
                measurementMilliseconds: 1000,
                drainMilliseconds: 0)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.False(gate.Passed);
        Assert.Contains(
            "throughput",
            Assert.Single(gate.Summaries).FailureReason);
    }

    [Fact]
    public void GateRejectsDeliveryGaps()
    {
        BrokerComparisonResult[] results =
        [
            Create(
                "mqttzero",
                throughput: 101,
                p99: 99,
                failures: 0,
                missingDeliveries: 1),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.False(gate.Passed);
        Assert.Contains(
            "gaps",
            Assert.Single(gate.Summaries).FailureReason);
    }

    [Fact]
    public void GateRejectsSequenceRegressionsAndDuplicates()
    {
        BrokerComparisonResult[] results =
        [
            Create(
                "mqttzero",
                throughput: 101,
                p99: 99,
                failures: 0,
                sequenceRegressions: 1,
                duplicateDeliveries: 1),
            Create("mqttnet", throughput: 100, p99: 100, failures: 0)
        ];

        BenchmarkGateResult gate = BenchmarkGateEvaluator.Evaluate(results);

        Assert.False(gate.Passed);
        string? reason = Assert.Single(gate.Summaries).FailureReason;
        Assert.Contains("ordered", reason);
        Assert.Contains("duplicates", reason);
    }

    private static BrokerComparisonResult Create(
        string broker,
        double throughput,
        double p99,
        long failures,
        long publishedMessages = 1,
        int subscribers = 1,
        long receivedMessages = 1,
        double? endToEndThroughput = null,
        double measurementMilliseconds = 1000,
        double drainMilliseconds = 0,
        long missingDeliveries = 0,
        long sequenceRegressions = 0,
        long duplicateDeliveries = 0)
    {
        return new BrokerComparisonResult
        {
            Broker = broker,
            Qos = 1,
            Subscribers = subscribers,
            PayloadSize = 32,
            Repetition = 1,
            BrokerObservation = new BenchmarkProcessObservation(
                0,
                0,
                0,
                0),
            Result = new BrokerBenchmarkResult
            {
                PublishedMessages = publishedMessages,
                ReceivedMessages = receivedMessages,
                FailedOperations = failures,
                WarmupPublishedMessages = 1,
                WarmupReceivedMessages = 1,
                MeasurementReceivedMessages = receivedMessages,
                PublishWindowThroughputPerSecond = throughput,
                EndToEndThroughputPerSecond =
                    endToEndThroughput ?? throughput,
                PublishCallLatencyP50Microseconds = p99,
                PublishCallLatencyP95Microseconds = p99,
                PublishCallLatencyP99Microseconds = p99,
                MissingDeliveryMessages = missingDeliveries,
                SequenceRegressions = sequenceRegressions,
                DuplicateDeliveryMessages = duplicateDeliveries,
                LoadObservation = new BenchmarkLoadObservation(
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0),
                MeasurementMilliseconds = measurementMilliseconds,
                MeasurementDrainMilliseconds = drainMilliseconds
            }
        };
    }
}
