using MqttZero.Server.Egress;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressBatchPolicyTests
{
    [Theory]
    [InlineData(8 * 1024, 512 * 1024, 128)]
    [InlineData(32 * 1024, 128 * 1024, 2)]
    [InlineData(64 * 1024, 128 * 1024, 2)]
    public void ResolveUsesAdaptiveLargeMessageLimits(
        int encodedLength,
        int expectedBytes,
        int expectedMessages)
    {
        (int bytes, int messages) =
            MqttZeroEgressBatchPolicy.Resolve(
                encodedLength,
                activeConnectionCount: 100,
                configuredBytes: 512 * 1024,
                configuredMessages: 128);

        Assert.Equal(expectedBytes, bytes);
        Assert.Equal(expectedMessages, messages);
    }

    [Fact]
    public void ResolvePreservesSmallerConfiguredLimits()
    {
        (int bytes, int messages) =
            MqttZeroEgressBatchPolicy.Resolve(
                encodedLength: 64 * 1024,
                activeConnectionCount: 100,
                configuredBytes: 64 * 1024,
                configuredMessages: 1);

        Assert.Equal(64 * 1024, bytes);
        Assert.Equal(1, messages);
    }

    [Fact]
    public void ResolveKeepsConfiguredLimitsForSingleClient()
    {
        (int bytes, int messages) =
            MqttZeroEgressBatchPolicy.Resolve(
                encodedLength: 64 * 1024,
                activeConnectionCount: 2,
                configuredBytes: 512 * 1024,
                configuredMessages: 128);

        Assert.Equal(512 * 1024, bytes);
        Assert.Equal(128, messages);
    }
}
