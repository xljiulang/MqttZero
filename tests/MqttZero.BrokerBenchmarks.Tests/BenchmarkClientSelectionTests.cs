using MqttZero.BrokerBenchmarks;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkClientSelectionTests
{
    [Theory]
    [InlineData("mqttnet", BenchmarkClientKind.MqttNet)]
    [InlineData("RAW", BenchmarkClientKind.Raw)]
    public void ParsesBenchmarkClientKind(
        string value,
        BenchmarkClientKind expected)
    {
        Assert.Equal(expected, BenchmarkClientSelection.Parse(value));
    }

    [Fact]
    public void RejectsUnknownBenchmarkClientKind()
    {
        Assert.Throws<ArgumentException>(
            () => BenchmarkClientSelection.Parse("other"));
    }
}
