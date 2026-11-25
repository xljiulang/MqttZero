using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroShardCountResolverTests
{
    [Theory]
    [InlineData(1, 1)]
    [InlineData(8, 8)]
    [InlineData(256, 256)]
    public void ExplicitShardCountWins(
        int configured,
        int expected)
    {
        int actual = MqttZeroShardCountResolver.Resolve(
            configured);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void AutoUsesLogicalProcessorCount()
    {
        int actual = MqttZeroShardCountResolver.Resolve(0);

        Assert.Equal(
            Math.Clamp(Environment.ProcessorCount, 1, 256),
            actual);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(257)]
    public void RejectsOutOfRangeConfiguredCount(int configured)
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => MqttZeroShardCountResolver.Resolve(
                configured));
    }
}
