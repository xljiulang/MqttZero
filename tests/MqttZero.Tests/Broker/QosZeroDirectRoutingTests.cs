using MqttZero.Server.Broker;

namespace MqttZero.Tests.Broker;

public sealed class QosZeroDirectRoutingTests
{
    [Fact]
    public void DirectRouteIsAllowedWhenNothingIsPending()
    {
        Assert.True(MqttZeroQosZeroDirectRouting.CanRoute(
            pendingPublishCount: 0,
            retainFlag: false,
            anyStoreEnabled: false,
            anyExtensionRegistered: false));
    }

    [Fact]
    public void DeferredPublishBlocksTheDirectRoute()
    {
        Assert.False(MqttZeroQosZeroDirectRouting.CanRoute(
            pendingPublishCount: 1,
            retainFlag: false,
            anyStoreEnabled: false,
            anyExtensionRegistered: false));
    }

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, true)]
    public void RetainStoresAndExtensionsBlockTheDirectRoute(
        bool retainFlag,
        bool anyStoreEnabled,
        bool anyExtensionRegistered)
    {
        Assert.False(MqttZeroQosZeroDirectRouting.CanRoute(
            pendingPublishCount: 0,
            retainFlag,
            anyStoreEnabled,
            anyExtensionRegistered));
    }
}
