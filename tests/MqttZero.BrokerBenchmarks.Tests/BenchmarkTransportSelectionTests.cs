using MqttZero.BrokerBenchmarks;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkTransportSelectionTests
{
    [Fact]
    public void ParseDefaultsToKestrel()
    {
        BenchmarkTransportKind transport =
            BenchmarkTransportSelection.Parse(
                new Dictionary<string, string>(
                    StringComparer.OrdinalIgnoreCase));

        Assert.Equal(BenchmarkTransportKind.Kestrel, transport);
    }

    [Theory]
    [InlineData("kestrel", "Kestrel")]
    [InlineData("socket", "Socket")]
    [InlineData("SOCKET", "Socket")]
    public void ParseAcceptsSupportedTransports(
        string value,
        string expected)
    {
        BenchmarkTransportKind transport =
            BenchmarkTransportSelection.Parse(
                new Dictionary<string, string>(
                    StringComparer.OrdinalIgnoreCase)
                {
                    ["transport"] = value
                });

        Assert.Equal(
            Enum.Parse<BenchmarkTransportKind>(expected),
            transport);
    }

    [Fact]
    public void ParseRejectsUnknownTransport()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(() =>
            BenchmarkTransportSelection.Parse(
                new Dictionary<string, string>(
                    StringComparer.OrdinalIgnoreCase)
                {
                    ["transport"] = "quic"
                }));

        Assert.Contains("quic", exception.Message, StringComparison.Ordinal);
    }
}
