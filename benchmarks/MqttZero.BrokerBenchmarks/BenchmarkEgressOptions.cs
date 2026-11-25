using MqttZero.Server;
using System.Globalization;

namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkEgressOptions(
    int OutboundBatchBytes,
    int OutboundBatchMessages,
    int OutboundBatchDurationMicroseconds)
{
    private const int DefaultBytes = 512 * 1024;
    private const int DefaultMessages = 128;
    private const int DefaultDurationMicroseconds = 500;

    public static BenchmarkEgressOptions Parse(
        IReadOnlyDictionary<string, string> arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        return new BenchmarkEgressOptions(
            ParsePositive(arguments, "outbound-batch-bytes", DefaultBytes),
            ParsePositive(
                arguments,
                "outbound-batch-messages",
                DefaultMessages),
            ParsePositive(
                arguments,
                "outbound-batch-duration-us",
                DefaultDurationMicroseconds));
    }

    public void ApplyTo(MqttZeroOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.OutboundBatchBytes = OutboundBatchBytes;
        options.OutboundBatchMessages = OutboundBatchMessages;
        options.OutboundBatchDuration = TimeSpan.FromMicroseconds(
            OutboundBatchDurationMicroseconds);
    }

    private static int ParsePositive(
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
            parsed <= 0)
        {
            throw new ArgumentOutOfRangeException(
                name,
                value,
                "The value must be greater than zero.");
        }

        return parsed;
    }
}
