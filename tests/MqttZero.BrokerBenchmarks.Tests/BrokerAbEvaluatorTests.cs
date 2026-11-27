using MqttZero.BrokerBenchmarks;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BrokerAbEvaluatorTests
{
    private static readonly BrokerAbVariantInfo Baseline =
        new("baseline", "baseline.dll", "1111111");

    private static readonly BrokerAbVariantInfo Candidate =
        new("candidate", "candidate.dll", "2222222");

    [Fact]
    public void PassesWhenCandidateImprovesThroughputWithoutLatencyRegression()
    {
        BrokerAbRoundResult[] rounds =
        [
            Round(1, "baseline", throughput: 100, deliveryP99: 1_000),
            Round(1, "candidate", throughput: 112, deliveryP99: 1_020),
            Round(2, "candidate", throughput: 114, deliveryP99: 1_040),
            Round(2, "baseline", throughput: 102, deliveryP99: 1_010)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            Baseline,
            Candidate,
            rounds);

        Assert.True(summary.Passed, summary.FailureReason);
        Assert.Equal(
            113,
            summary.CandidateEndToEndThroughput,
            precision: 6);
        Assert.Equal(101, summary.BaselineEndToEndThroughput, precision: 6);
    }

    [Fact]
    public void FailsWhenThroughputGainIsBelowTenPercent()
    {
        BrokerAbRoundResult[] rounds =
        [
            Round(1, "baseline", throughput: 100),
            Round(1, "candidate", throughput: 105),
            Round(2, "candidate", throughput: 107),
            Round(2, "baseline", throughput: 100)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            Baseline,
            Candidate,
            rounds);

        Assert.False(summary.Passed);
        Assert.Contains("throughput", summary.FailureReason);
    }

    [Fact]
    public void FailsWhenDeliveryP99RegressesBeyondFivePercent()
    {
        BrokerAbRoundResult[] rounds =
        [
            Round(1, "baseline", throughput: 100, deliveryP99: 1_000),
            Round(1, "candidate", throughput: 120, deliveryP99: 1_100),
            Round(2, "candidate", throughput: 120, deliveryP99: 1_100),
            Round(2, "baseline", throughput: 100, deliveryP99: 1_000)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            Baseline,
            Candidate,
            rounds);

        Assert.False(summary.Passed);
        Assert.Contains("delivery p99", summary.FailureReason);
    }

    [Fact]
    public void FailsWhenADeliveryIsMissing()
    {
        BrokerAbRoundResult[] rounds =
        [
            Round(1, "baseline", throughput: 100, missing: 0),
            Round(1, "candidate", throughput: 120, missing: 1)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            Baseline,
            Candidate,
            rounds);

        Assert.False(summary.Passed);
        Assert.Contains("Missing", summary.FailureReason);
        Assert.Equal(1, summary.MissingDeliveryMessages);
    }

    [Fact]
    public void FailsWhenVariantsRunDifferentRoundCounts()
    {
        BrokerAbRoundResult[] rounds =
        [
            Round(1, "baseline", throughput: 100),
            Round(1, "candidate", throughput: 120),
            Round(2, "baseline", throughput: 101)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            Baseline,
            Candidate,
            rounds);

        Assert.False(summary.Passed);
        Assert.Contains("same number of rounds", summary.FailureReason);
    }

    [Fact]
    public void DrainDominatedCellComparesPublishWindowThroughput()
    {
        BrokerAbRoundResult[] rounds =
        [
            Round(
                1,
                "baseline",
                throughput: 100,
                drainRatio: 0.5,
                publishWindowThroughput: 400),
            Round(
                1,
                "candidate",
                throughput: 120,
                drainRatio: 0.5,
                publishWindowThroughput: 440)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            Baseline,
            Candidate,
            rounds,
            minimumThroughputGain: -0.05);

        Assert.True(summary.Passed, summary.FailureReason);
        Assert.Equal("publish-window", summary.ThroughputMetric);
        Assert.Equal(0.5, summary.DrainRatio, precision: 3);
        Assert.Equal(400, summary.BaselineEndToEndThroughput, precision: 3);
        Assert.Equal(440, summary.CandidateEndToEndThroughput, precision: 3);
    }

    [Fact]
    public void ShortDrainCellComparesEndToEndThroughput()
    {
        BrokerAbRoundResult[] rounds =
        [
            Round(1, "baseline", throughput: 100, drainRatio: 0.01),
            Round(1, "candidate", throughput: 105, drainRatio: 0.01)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            Baseline,
            Candidate,
            rounds,
            minimumThroughputGain: -0.05);

        Assert.True(summary.Passed, summary.FailureReason);
        Assert.Equal("end-to-end", summary.ThroughputMetric);
    }

    [Fact]
    public void AbsoluteDeliveryP99BudgetReplacesTheRatioGate()
    {
        BrokerAbRoundResult[] rounds =
        [
            Round(1, "baseline", throughput: 100, deliveryP99: 900),
            Round(1, "candidate", throughput: 100, deliveryP99: 2_000)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            Baseline,
            Candidate,
            rounds,
            minimumThroughputGain: -0.05,
            maximumDeliveryP99Microseconds: 2_500);

        Assert.True(summary.Passed, summary.FailureReason);
        Assert.True(summary.DeliveryP99Ratio > 1.05);
    }

    [Fact]
    public void MismatchedShardRequestsFailTheReport()
    {
        BrokerAbVariantInfo baseline = new(
            "baseline",
            "baseline.dll",
            "1111111",
            new BenchmarkTopologyOptions(8, 0));
        BrokerAbVariantInfo candidate = new(
            "candidate",
            "candidate.dll",
            "2222222",
            new BenchmarkTopologyOptions(12, 0));
        BrokerAbRoundResult[] rounds =
        [
            Round(1, "baseline", throughput: 100),
            Round(1, "candidate", throughput: 100)
        ];

        BrokerAbSummary summary = BrokerAbEvaluator.Evaluate(
            baseline,
            candidate,
            rounds,
            minimumThroughputGain: -0.05);

        Assert.False(summary.Passed);
        Assert.Contains(
            "same event loop shard count",
            summary.FailureReason);
    }

    private static BrokerAbRoundResult Round(
        int round,
        string variant,
        double throughput,
        double deliveryP99 = 0,
        long missing = 0,
        double drainRatio = 0,
        double publishWindowThroughput = 0)
    {
        return new BrokerAbRoundResult
        {
            Round = round,
            Variant = variant,
            Result = new BrokerBenchmarkResult
            {
                PublishedMessages = 10,
                Publishers = 1,
                ReceivedMessages = 10,
                MeasurementReceivedMessages = 10,
                FailedOperations = 0,
                PublishWindowThroughputPerSecond = publishWindowThroughput,
                EndToEndThroughputPerSecond = throughput,
                DeliveryLatencyP99Microseconds = deliveryP99,
                MissingDeliveryMessages = missing,
                DrainRatio = drainRatio,
                LoadObservation = new BenchmarkLoadObservation(
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0)
            },
            BrokerObservation = new BenchmarkProcessObservation(0, 0, 0, 0),
            ClientObservation = new BenchmarkProcessObservation(0, 0, 0, 0)
        };
    }
}
