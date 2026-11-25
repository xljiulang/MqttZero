using MqttZero.Server.Egress;

namespace MqttZero.Tests.Egress;

public sealed class MqttZeroEgressBatchBudgetTests
{
    [Fact]
    public void RejectsMessageCountBeyondLimit()
    {
        MqttZeroEgressBatchBudget budget = new(
            maximumBytes: 1024,
            maximumMessages: 2,
            deadlineTimestamp: 100);

        Assert.True(budget.TryAdd(encodedBytes: 10, timestamp: 0));
        Assert.True(budget.TryAdd(encodedBytes: 10, timestamp: 0));
        Assert.False(budget.TryAdd(encodedBytes: 10, timestamp: 0));
        Assert.Equal(2, budget.MessageCount);
        Assert.Equal(20, budget.EncodedBytes);
    }

    [Fact]
    public void RejectsNextMessageThatExceedsByteLimit()
    {
        MqttZeroEgressBatchBudget budget = new(
            maximumBytes: 100,
            maximumMessages: 10,
            deadlineTimestamp: 100);

        Assert.True(budget.TryAdd(encodedBytes: 80, timestamp: 0));
        Assert.False(budget.TryAdd(encodedBytes: 21, timestamp: 0));
        Assert.Equal(1, budget.MessageCount);
        Assert.Equal(80, budget.EncodedBytes);
    }

    [Fact]
    public void AllowsFirstMessageToExceedByteLimit()
    {
        MqttZeroEgressBatchBudget budget = new(
            maximumBytes: 100,
            maximumMessages: 10,
            deadlineTimestamp: 100);

        Assert.True(budget.TryAdd(encodedBytes: 101, timestamp: 0));
        Assert.False(budget.TryAdd(encodedBytes: 1, timestamp: 0));
    }

    [Fact]
    public void RejectsNextMessageAtDurationDeadline()
    {
        MqttZeroEgressBatchBudget budget = new(
            maximumBytes: 1024,
            maximumMessages: 10,
            deadlineTimestamp: 100);

        Assert.True(budget.TryAdd(encodedBytes: 10, timestamp: 99));
        Assert.False(budget.TryAdd(encodedBytes: 10, timestamp: 100));
        Assert.Equal(1, budget.MessageCount);
    }
}
