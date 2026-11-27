using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MQTTnet.Protocol;
using MqttZero.Server.Broker;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task AddingOneSubscriptionKeepsTheExistingThousand()
        {
            const int ExistingSubscriptions = 1_000;
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options => options.MaxSubscriptionsPerSession = 2_000);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += message =>
            {
                received.TrySetResult(message.ApplicationMessage.Topic);
                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "diff-subscriber"),
                cancellationToken);
            MqttClientSubscribeOptionsBuilder builder =
                new MqttClientSubscribeOptionsBuilder();
            for (int index = 0; index < ExistingSubscriptions; index++)
            {
                _ = builder.WithTopicFilter(
                    $"diff/{index}",
                    MqttQualityOfServiceLevel.AtLeastOnce);
            }

            await subscriber.SubscribeAsync(
                builder.Build(),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "diff/extra",
                        MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            Assert.Equal(
                ExistingSubscriptions + 1,
                broker.GetSessionSnapshots()
                    .Single(session =>
                        session.ClientId == "diff-subscriber")
                    .SubscriptionCount);

            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(
                CreateClientOptions(port, "diff-publisher"),
                cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("diff/0")
                    .WithPayload(new byte[16])
                    .WithQualityOfServiceLevel(
                        MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);
            Assert.Equal(
                "diff/0",
                await received.Task.WaitAsync(
                    TimeSpan.FromSeconds(10),
                    cancellationToken));
        }
    }
}
