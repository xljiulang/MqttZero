namespace MqttZero.BrokerBenchmarks;

internal static class BrokerAbEvaluator
{
    public const double MinimumThroughputGain = 0.10;

    public const double MaximumDeliveryP99Regression = 0.05;

    public static BrokerAbSummary Evaluate(
        BrokerAbVariantInfo baseline,
        BrokerAbVariantInfo candidate,
        IReadOnlyList<BrokerAbRoundResult> rounds,
        double minimumThroughputGain = MinimumThroughputGain,
        double maximumDeliveryP99Regression =
            MaximumDeliveryP99Regression,
        double maximumDeliveryP99Microseconds = 0,
        double maximumDrainRatio = 0.10)
    {
        ArgumentNullException.ThrowIfNull(baseline);
        ArgumentNullException.ThrowIfNull(candidate);
        ArgumentNullException.ThrowIfNull(rounds);

        BrokerAbRoundResult[] baselineRounds = rounds
            .Where(round => round.Variant == baseline.Name)
            .ToArray();
        BrokerAbRoundResult[] candidateRounds = rounds
            .Where(round => round.Variant == candidate.Name)
            .ToArray();

        List<string> failureReasons = [];
        if (baselineRounds.Length == 0 || candidateRounds.Length == 0)
        {
            failureReasons.Add(
                "Both variants require at least one completed round.");
        }

        if (baselineRounds.Length != candidateRounds.Length)
        {
            failureReasons.Add(
                "Both variants must run the same number of rounds.");
        }

        long failures = rounds.Sum(
            static round => round.Result.FailedOperations);
        long sequenceRegressions = rounds.Sum(
            static round => round.Result.SequenceRegressions);
        long missingDeliveries = rounds.Sum(
            static round => round.Result.MissingDeliveryMessages);
        long duplicateDeliveries = rounds.Sum(
            static round => round.Result.DuplicateDeliveryMessages);
        if (failures != 0)
        {
            failureReasons.Add($"Failed operations must be zero: {failures}.");
        }

        if (missingDeliveries != 0)
        {
            failureReasons.Add(
                $"Missing deliveries must be zero: {missingDeliveries}.");
        }

        if (sequenceRegressions != 0)
        {
            failureReasons.Add(
                $"Sequence regressions must be zero: {sequenceRegressions}.");
        }

        if (duplicateDeliveries != 0)
        {
            failureReasons.Add(
                $"Duplicate deliveries must be zero: {duplicateDeliveries}.");
        }

        if (baseline.Topology?.EventLoopShardCount !=
            candidate.Topology?.EventLoopShardCount)
        {
            failureReasons.Add(
                "Both variants must request the same event loop shard count.");
        }

        double drainRatio = Median(
            rounds.Select(static round => round.Result.DrainRatio));
        bool usePublishWindow = drainRatio > maximumDrainRatio;
        string throughputMetric = usePublishWindow
            ? "publish-window"
            : "end-to-end";
        double baselineThroughput = Median(
            baselineRounds.Select(
                round => GetThroughput(round, usePublishWindow)));
        double candidateThroughput = Median(
            candidateRounds.Select(
                round => GetThroughput(round, usePublishWindow)));
        double baselinePublishP99 = Median(
            baselineRounds.Select(
                static round =>
                    round.Result.PublishCallLatencyP99Microseconds));
        double candidatePublishP99 = Median(
            candidateRounds.Select(
                static round =>
                    round.Result.PublishCallLatencyP99Microseconds));
        double baselineDeliveryP99 = Median(
            baselineRounds.Select(
                static round =>
                    round.Result.DeliveryLatencyP99Microseconds));
        double candidateDeliveryP99 = Median(
            candidateRounds.Select(
                static round =>
                    round.Result.DeliveryLatencyP99Microseconds));

        double throughputChange = baselineThroughput > 0
            ? (candidateThroughput - baselineThroughput) /
                baselineThroughput
            : 0;
        double deliveryP99Ratio = baselineDeliveryP99 > 0
            ? candidateDeliveryP99 / baselineDeliveryP99
            : 0;

        if (baselineThroughput <= 0 || candidateThroughput <= 0)
        {
            failureReasons.Add(
                $"Both variants must report positive {throughputMetric} " +
                "throughput.");
        }
        else if (throughputChange < minimumThroughputGain)
        {
            failureReasons.Add(
                $"Candidate {throughputMetric} throughput must improve by " +
                $"at least " +
                $"{minimumThroughputGain:P0}: actual={throughputChange:P1}.");
        }

        if (maximumDeliveryP99Microseconds > 0)
        {
            if (candidateDeliveryP99 > maximumDeliveryP99Microseconds)
            {
                failureReasons.Add(
                    "Candidate delivery p99 must stay below " +
                    $"{maximumDeliveryP99Microseconds:F0} us: " +
                    $"actual={candidateDeliveryP99:F0} us.");
            }
        }
        else if (baselineDeliveryP99 > 0 &&
            deliveryP99Ratio > 1 + maximumDeliveryP99Regression)
        {
            failureReasons.Add(
                $"Candidate delivery p99 must not regress by more than " +
                $"{maximumDeliveryP99Regression:P0}: " +
                $"actual={(deliveryP99Ratio - 1):P1}.");
        }

        return new BrokerAbSummary(
            baselineThroughput,
            candidateThroughput,
            throughputChange,
            baselinePublishP99,
            candidatePublishP99,
            baselineDeliveryP99,
            candidateDeliveryP99,
            deliveryP99Ratio,
            failures,
            sequenceRegressions,
            missingDeliveries,
            duplicateDeliveries,
            throughputMetric,
            drainRatio,
            failureReasons.Count == 0,
            failureReasons.Count == 0
                ? null
                : string.Join(" ", failureReasons));
    }

    private static double GetThroughput(
        BrokerAbRoundResult round,
        bool usePublishWindow)
    {
        return usePublishWindow
            ? round.Result.PublishWindowThroughputPerSecond
            : round.Result.EndToEndThroughputPerSecond;
    }

    private static double Median(IEnumerable<double> values)
    {
        double[] ordered = values.Order().ToArray();
        if (ordered.Length == 0)
        {
            return 0;
        }

        int middle = ordered.Length / 2;
        return ordered.Length % 2 == 0
            ? (ordered[middle - 1] + ordered[middle]) / 2
            : ordered[middle];
    }
}
