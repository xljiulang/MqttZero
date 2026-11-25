using MqttZero.BrokerBenchmarks;
using MqttZero.Protocol;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class RawMqttClientTests
{
    [Fact(Timeout = 30_000)]
    public async Task RawClientPublishesQosOneMessageToSubscriber()
    {
        await using TestBrokerHost broker = await TestBrokerHost.StartAsync();
        await using RawMqttClient subscriber = broker.CreateRawClient();
        await using RawMqttClient publisher = broker.CreateRawClient();

        await subscriber.ConnectAsync(
            "raw-subscriber",
            TestContext.Current.CancellationToken);
        await subscriber.SubscribeAsync(
            "bench/topic",
            MqttQualityOfServiceLevel.AtLeastOnce,
            TestContext.Current.CancellationToken);
        await publisher.ConnectAsync(
            "raw-publisher",
            TestContext.Current.CancellationToken);

        await publisher.PublishAsync(
            "bench/topic",
            new byte[64],
            MqttQualityOfServiceLevel.AtLeastOnce,
            TestContext.Current.CancellationToken);

        await TestBrokerHost.WaitUntilAsync(
            () => subscriber.ReceivedMessageCount == 1,
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
        Assert.Equal(1, subscriber.ReceivedMessageCount);
    }

    [Fact(Timeout = 30_000)]
    public async Task RawClientPublishesQosZeroMessageToSubscriber()
    {
        await using TestBrokerHost broker = await TestBrokerHost.StartAsync();
        await using RawMqttClient subscriber = broker.CreateRawClient();
        await using RawMqttClient publisher = broker.CreateRawClient();

        await subscriber.ConnectAsync(
            "raw-qos0-subscriber",
            TestContext.Current.CancellationToken);
        await subscriber.SubscribeAsync(
            "bench/topic",
            MqttQualityOfServiceLevel.AtMostOnce,
            TestContext.Current.CancellationToken);
        await publisher.ConnectAsync(
            "raw-qos0-publisher",
            TestContext.Current.CancellationToken);

        await publisher.PublishAsync(
            "bench/topic",
            new byte[32],
            MqttQualityOfServiceLevel.AtMostOnce,
            TestContext.Current.CancellationToken);

        await TestBrokerHost.WaitUntilAsync(
            () => subscriber.ReceivedMessageCount == 1,
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
        Assert.Equal(1, subscriber.ReceivedMessageCount);
    }

    [Fact]
    public async Task RawClientRejectsQosTwoBeforeNetworkWrite()
    {
        await using RawMqttClient client = new(new RawMqttClientOptions
        {
            Host = "127.0.0.1",
            Port = 1883,
            ClientId = "raw-qos2"
        });

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await client.PublishAsync(
                "bench/topic",
                ReadOnlyMemory<byte>.Empty,
                MqttQualityOfServiceLevel.ExactlyOnce,
                TestContext.Current.CancellationToken));
    }
}
