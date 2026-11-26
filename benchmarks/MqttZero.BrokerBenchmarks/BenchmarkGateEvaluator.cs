namespace MqttZero.BrokerBenchmarks;

internal static class BenchmarkGateEvaluator
{
    public static BenchmarkGateResult Evaluate(
        IReadOnlyList<BrokerComparisonResult> results,
        bool enforceRankingComparison = true)
    {
        ArgumentNullException.ThrowIfNull(results);

        List<BenchmarkSummary> summaries = [];
        List<string> errors = [];
        foreach (IGrouping<(int Qos, int Subscribers, int PayloadSize),
            BrokerComparisonResult> group in results.GroupBy(
                static result => (
                    result.Qos,
                    result.Subscribers,
                    result.PayloadSize)))
        {
            BrokerComparisonResult[] mqttZeroResults = group
                .Where(static result => result.Broker == "mqttzero")
                .ToArray();
            BrokerComparisonResult[] mqttNetResults = group
                .Where(static result => result.Broker == "mqttnet")
                .ToArray();

            List<string> failureReasons = [];
            if (mqttZeroResults.Length == 0)
            {
                failureReasons.Add("Missing mqttzero broker result.");
            }

            if (mqttNetResults.Length == 0)
            {
                failureReasons.Add("Missing mqttnet broker result.");
            }

            if (mqttZeroResults.Length > 0 &&
                mqttNetResults.Length > 0 &&
                mqttZeroResults.Length != mqttNetResults.Length)
            {
                failureReasons.Add(
                    "Broker repetition counts do not match.");
            }

            long mqttZeroFailures = mqttZeroResults.Sum(
                static result => result.Result.FailedOperations);
            long mqttNetFailures = mqttNetResults.Sum(
                static result => result.Result.FailedOperations);
            if (mqttZeroFailures != 0 || mqttNetFailures != 0)
            {
                failureReasons.Add(
                    $"Broker failure counts must be zero: mqttzero={mqttZeroFailures}, mqttnet={mqttNetFailures}.");
            }

            ValidateMeasurementCompletion(
                "mqttzero",
                mqttZeroResults,
                group.Key.Subscribers,
                failureReasons);
            ValidateMeasurementCompletion(
                "mqttnet",
                mqttNetResults,
                group.Key.Subscribers,
                failureReasons);

            double mqttZeroThroughput = Median(
                mqttZeroResults.Select(
                    static result =>
                        result.Result.EndToEndThroughputPerSecond));
            double mqttNetThroughput = Median(
                mqttNetResults.Select(
                    static result =>
                        result.Result.EndToEndThroughputPerSecond));
            double mqttZeroP99 = Median(
                mqttZeroResults.Select(
                    static result =>
                        result.Result.PublishCallLatencyP99Microseconds));
            double mqttNetP99 = Median(
                mqttNetResults.Select(
                    static result =>
                        result.Result.PublishCallLatencyP99Microseconds));

            double throughputRatio = 0;
            if (mqttNetThroughput > 0)
            {
                throughputRatio = mqttZeroThroughput / mqttNetThroughput;
                if (enforceRankingComparison && throughputRatio <= 1.0)
                {
                    failureReasons.Add(
                        "mqttzero throughput ratio must be greater than 1.0.");
                }
            }
            else
            {
                failureReasons.Add(
                    "mqttnet throughput must be positive.");
            }

            double p99Ratio = 0;
            if (mqttNetP99 > 0)
            {
                p99Ratio = mqttZeroP99 / mqttNetP99;
                if (enforceRankingComparison && p99Ratio >= 1.0)
                {
                    failureReasons.Add(
                        "mqttzero p99 ratio must be less than 1.0.");
                }
            }
            else
            {
                failureReasons.Add(
                    "mqttnet p99 latency must be positive.");
            }

            string? failureReason = failureReasons.Count == 0
                ? null
                : string.Join(" ", failureReasons);
            BenchmarkSummary summary = new(
                group.Key.Qos,
                group.Key.Subscribers,
                group.Key.PayloadSize,
                mqttZeroThroughput,
                mqttNetThroughput,
                throughputRatio,
                mqttZeroP99,
                mqttNetP99,
                p99Ratio,
                mqttZeroFailures,
                mqttNetFailures,
                failureReason == null,
                failureReason);
            summaries.Add(summary);
            if (failureReason != null)
            {
                errors.Add(
                    $"QoS={summary.Qos}, Subscribers={summary.Subscribers}, Payload={summary.PayloadSize}: {failureReason}");
            }
        }

        if (summaries.Count == 0)
        {
            errors.Add("No benchmark comparison cells were produced.");
        }

        return new BenchmarkGateResult(
            summaries,
            summaries.Count > 0 &&
                summaries.All(static summary => summary.Passed),
            errors);
    }

    private static void ValidateMeasurementCompletion(
        string broker,
        IReadOnlyList<BrokerComparisonResult> results,
        int subscribers,
        List<string> failureReasons)
    {
        foreach (BrokerComparisonResult result in results)
        {
            long expected = result.Result.PublishedMessages *
                subscribers;
            if (result.Result.MeasurementReceivedMessages != expected)
            {
                failureReasons.Add(
                    $"{broker} received messages must equal " +
                    $"published messages times subscribers: " +
                    $"expected={expected}, " +
                    $"actual={result.Result.MeasurementReceivedMessages}.");
            }

            if (result.Result.UnsequencedDeliveryMessages != 0)
            {
                failureReasons.Add(
                    $"{broker} deliveries must carry a benchmark sequence: " +
                    $"unsequenced={result.Result.UnsequencedDeliveryMessages}.");
            }

            if (result.Result.MissingDeliveryMessages != 0)
            {
                failureReasons.Add(
                    $"{broker} deliveries must not contain gaps: " +
                    $"missing={result.Result.MissingDeliveryMessages}.");
            }

            if (result.Result.Publishers <= 1 &&
                result.Result.SequenceRegressions != 0)
            {
                failureReasons.Add(
                    $"{broker} single-publisher deliveries must be ordered: " +
                    $"regressions={result.Result.SequenceRegressions}.");
            }

            if (result.Result.Publishers <= 1 &&
                result.Result.DuplicateDeliveryMessages != 0)
            {
                failureReasons.Add(
                    $"{broker} deliveries must not contain duplicates: " +
                    $"duplicates={result.Result.DuplicateDeliveryMessages}.");
            }
        }
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
