namespace MqttZero.BrokerBenchmarks;

internal static class BenchmarkClientSelection
{
    public static BenchmarkClientKind Parse(string? value)
    {
        if (string.Equals(value, "mqttnet", StringComparison.OrdinalIgnoreCase))
        {
            return BenchmarkClientKind.MqttNet;
        }

        if (string.Equals(value, "raw", StringComparison.OrdinalIgnoreCase))
        {
            return BenchmarkClientKind.Raw;
        }

        throw new ArgumentException(
            $"Unknown benchmark client kind '{value}'. Expected 'mqttnet' or 'raw'.",
            nameof(value));
    }
}
