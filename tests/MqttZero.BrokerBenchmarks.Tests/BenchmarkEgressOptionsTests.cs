using MqttZero.BrokerBenchmarks;
using MqttZero.Server;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkEgressOptionsTests
{
    [Fact]
    public void ParseUsesProductionDefaults()
    {
        BenchmarkEgressOptions options = BenchmarkEgressOptions.Parse(
            new Dictionary<string, string>(
                StringComparer.OrdinalIgnoreCase));

        Assert.Equal(512 * 1024, options.OutboundBatchBytes);
        Assert.Equal(128, options.OutboundBatchMessages);
        Assert.Equal(500, options.OutboundBatchDurationMicroseconds);
    }

    [Fact]
    public void ParseReadsExplicitBatchValues()
    {
        BenchmarkEgressOptions options = BenchmarkEgressOptions.Parse(
            new Dictionary<string, string>(
                StringComparer.OrdinalIgnoreCase)
            {
                ["outbound-batch-bytes"] = "262144",
                ["outbound-batch-messages"] = "64",
                ["outbound-batch-duration-us"] = "250"
            });

        Assert.Equal(256 * 1024, options.OutboundBatchBytes);
        Assert.Equal(64, options.OutboundBatchMessages);
        Assert.Equal(250, options.OutboundBatchDurationMicroseconds);
    }

    [Theory]
    [InlineData("outbound-batch-bytes", "0")]
    [InlineData("outbound-batch-messages", "0")]
    [InlineData("outbound-batch-duration-us", "0")]
    public void ParseRejectsNonPositiveValues(
        string name,
        string value)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            BenchmarkEgressOptions.Parse(
                new Dictionary<string, string>(
                    StringComparer.OrdinalIgnoreCase)
                {
                    [name] = value
                }));
    }

    [Fact]
    public void ApplyOverridesBrokerOptions()
    {
        BenchmarkEgressOptions benchmarkOptions = new(
            OutboundBatchBytes: 256 * 1024,
            OutboundBatchMessages: 64,
            OutboundBatchDurationMicroseconds: 250);
        MqttZeroOptions brokerOptions = new();

        benchmarkOptions.ApplyTo(brokerOptions);

        Assert.Equal(256 * 1024, brokerOptions.OutboundBatchBytes);
        Assert.Equal(64, brokerOptions.OutboundBatchMessages);
        Assert.Equal(
            TimeSpan.FromMicroseconds(250),
            brokerOptions.OutboundBatchDuration);
    }
}
