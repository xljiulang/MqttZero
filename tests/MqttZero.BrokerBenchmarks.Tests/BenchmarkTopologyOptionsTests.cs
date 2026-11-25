using MqttZero.BrokerBenchmarks;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkTopologyOptionsTests
{
    [Fact]
    public void ParseUsesAutomaticTopologyByDefault()
    {
        BenchmarkTopologyOptions options =
            BenchmarkTopologyOptions.Parse(
                new Dictionary<string, string>(
                    StringComparer.OrdinalIgnoreCase));

        Assert.Equal(0, options.EventLoopShardCount);
        Assert.Equal(0, options.IoQueueCount);
    }

    [Theory]
    [InlineData("event-loop-shards", "257")]
    [InlineData("event-loop-shards", "-1")]
    [InlineData("io-queue-count", "257")]
    [InlineData("io-queue-count", "-1")]
    public void ParseRejectsOutOfRangeTopology(
        string argument,
        string value)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            BenchmarkTopologyOptions.Parse(
                new Dictionary<string, string>(
                    StringComparer.OrdinalIgnoreCase)
                {
                    [argument] = value
                }));
    }

    [Fact]
    public void CreateInfoResolvesAutomaticTopology()
    {
        BenchmarkTopologyOptions options = new(4, 2);

        BenchmarkTopologyInfo info = options.CreateInfo();

        Assert.Equal(4, info.EventLoopShardCount);
        Assert.Equal(4, info.ResolvedEventLoopShardCount);
        Assert.Equal(2, info.IoQueueCount);
        Assert.Equal(2, info.ResolvedIoQueueCount);
    }
}
