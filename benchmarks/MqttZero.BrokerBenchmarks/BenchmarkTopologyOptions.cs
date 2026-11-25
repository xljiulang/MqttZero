using System.Globalization;

namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkTopologyOptions(
    int EventLoopShardCount,
    int IoQueueCount)
{
    private const int MaximumCount = 256;

    public static BenchmarkTopologyOptions Parse(
        IReadOnlyDictionary<string, string> arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        int eventLoopShardCount = ParseCount(
            arguments,
            "event-loop-shards",
            0);
        int ioQueueCount = ParseCount(
            arguments,
            "io-queue-count",
            0);
        return new BenchmarkTopologyOptions(
            eventLoopShardCount,
            ioQueueCount);
    }

    public BenchmarkTopologyInfo CreateInfo()
    {
        int resolvedEventLoopShardCount =
            EventLoopShardCount == 0
                ? Math.Clamp(
                    Environment.ProcessorCount,
                    1,
                    MaximumCount)
                : EventLoopShardCount;
        int resolvedIoQueueCount = IoQueueCount == 0
            ? resolvedEventLoopShardCount
            : IoQueueCount;
        return new BenchmarkTopologyInfo(
            EventLoopShardCount,
            resolvedEventLoopShardCount,
            IoQueueCount,
            resolvedIoQueueCount);
    }

    private static int ParseCount(
        IReadOnlyDictionary<string, string> arguments,
        string name,
        int defaultValue)
    {
        if (!arguments.TryGetValue(name, out string? value))
        {
            return defaultValue;
        }

        if (!int.TryParse(
            value,
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out int parsed) ||
            parsed < 0 ||
            parsed > MaximumCount)
        {
            throw new ArgumentOutOfRangeException(
                name,
                value,
                $"The value must be between 0 and {MaximumCount}.");
        }

        return parsed;
    }
}
