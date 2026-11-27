using MqttZero.Server.Egress;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressFairnessTests
{
    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    [InlineData(63, 63)]
    [InlineData(64, 64)]
    [InlineData(65, 64)]
    [InlineData(1024, 64)]
    public void ControlBatchNeverExceedsThePerPassBudget(
        int queuedCount,
        int expected)
    {
        Assert.Equal(
            expected,
            MqttZeroEgressFairness.GetControlBatchSize(queuedCount));
    }

    [Theory]
    [InlineData(0, true, false)]
    [InlineData(63, true, false)]
    [InlineData(64, true, true)]
    [InlineData(128, true, true)]
    [InlineData(64, false, false)]
    public void DataDrainsOnceTheControlBudgetIsSpent(
        int controlFramesSinceDataPass,
        bool dataSignaled,
        bool expected)
    {
        Assert.Equal(
            expected,
            MqttZeroEgressFairness.ShouldDrainData(
                controlFramesSinceDataPass,
                dataSignaled));
    }

    [Fact]
    public void BudgetIsSixtyFourControlFrames()
    {
        Assert.Equal(
            64,
            MqttZeroEgressFairness.MaximumControlFramesPerPass);
    }
}
