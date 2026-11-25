namespace MqttZero.BrokerBenchmarks;

internal static class BenchmarkTransportSelection
{
    public static BenchmarkTransportKind Parse(
        IReadOnlyDictionary<string, string> arguments)
    {
        ArgumentNullException.ThrowIfNull(arguments);
        if (!arguments.TryGetValue("transport", out string? value))
        {
            return BenchmarkTransportKind.Kestrel;
        }

        return value.ToLowerInvariant() switch
        {
            "kestrel" => BenchmarkTransportKind.Kestrel,
            "socket" => BenchmarkTransportKind.Socket,
            _ => throw new ArgumentException(
                $"Unsupported transport '{value}'.")
        };
    }
}
