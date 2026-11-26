using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task NetworkAndManagementSubscriptionsShareOneEvaluation()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            await client.ConnectAsync(
                CreateClientOptions(port, "parity-client"),
                TestContext.Current.CancellationToken);
            if (!broker.TryGetClient(
                "parity-client",
                out MqttZeroConnectedClient? managed) ||
                managed == null)
            {
                throw new InvalidOperationException("The connected client handle was not available.");
            }

            MqttClientSubscribeResult networkSubscribe = await client.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "parity/net/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                TestContext.Current.CancellationToken);
            IReadOnlyCollection<MqttClientSubscribeResultItem> networkItems =
                networkSubscribe.Items ?? [];
            Assert.Equal(
                MqttClientSubscribeResultCode.GrantedQoS1,
                Assert.Single(networkItems).ResultCode);

            IReadOnlyList<MqttZeroSubscribeResult> managedSubscribe =
                await managed.SubscribeAsync(
                    [
                        new MqttSubscriptionRequest
                        {
                            TopicFilter = new ReadOnlySequence<byte>("parity/managed/#"u8.ToArray()),
                            QualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce
                        }
                    ],
                    cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.Success,
                Assert.Single(managedSubscribe).ReasonCode);
            Assert.Equal(
                2,
                GetSubscriptionCount(broker, "parity-client"));

            MqttClientSubscribeResult networkReplace = await client.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "parity/managed/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                    .Build(),
                TestContext.Current.CancellationToken);
            IReadOnlyCollection<MqttClientSubscribeResultItem> replaceItems =
                networkReplace.Items ?? [];
            Assert.Equal(
                MqttClientSubscribeResultCode.GrantedQoS2,
                Assert.Single(replaceItems).ResultCode);
            Assert.Equal(
                2,
                GetSubscriptionCount(broker, "parity-client"));

            IReadOnlyList<MqttZeroUnsubscribeResult> managedUnsubscribe =
                await managed.UnsubscribeAsync(
                    ["parity/net/#"],
                    TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.Success,
                Assert.Single(managedUnsubscribe).ReasonCode);
            IReadOnlyList<MqttZeroUnsubscribeResult> repeatedUnsubscribe =
                await managed.UnsubscribeAsync(
                    ["parity/net/#"],
                    TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.NoSubscriptionExisted,
                Assert.Single(repeatedUnsubscribe).ReasonCode);
            Assert.Equal(
                1,
                GetSubscriptionCount(broker, "parity-client"));
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task ManagementSubscribeDeliversRetainedMessageProperties()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(
                CreateClientOptions(port, "retained-publisher"),
                TestContext.Current.CancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("parity/retained")
                    .WithPayload("value"u8.ToArray())
                    .WithRetainFlag()
                    .WithContentType("text/plain")
                    .WithPayloadFormatIndicator(
                        MQTTnet.Protocol.MqttPayloadFormatIndicator.CharacterData)
                    .Build(),
                TestContext.Current.CancellationToken);
            await WaitForRetainedCountAsync(broker, 1);

            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<MqttApplicationMessage> received = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage);
                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "retained-subscriber"),
                TestContext.Current.CancellationToken);
            if (!broker.TryGetClient(
                "retained-subscriber",
                out MqttZeroConnectedClient? managed) ||
                managed == null)
            {
                throw new InvalidOperationException("The connected client handle was not available.");
            }

            IReadOnlyList<MqttZeroSubscribeResult> results = await managed.SubscribeAsync(
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("parity/retained"u8.ToArray()),
                        QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                    }
                ],
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(MqttReasonCode.GrantedQos1, Assert.Single(results).ReasonCode);

            MqttApplicationMessage message = await received.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            Assert.Equal("value", message.ConvertPayloadToString());
            Assert.Equal("text/plain", message.ContentType);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        private static int GetSubscriptionCount(MqttZeroBroker broker, string clientId)
        {
            MqttZeroSessionSnapshot session = broker.GetSessions()
                .Single(item => item.ClientId == clientId);
            return session.SubscriptionCount;
        }

        private static async Task WaitForRetainedCountAsync(
            MqttZeroBroker broker,
            int expected)
        {
            for (int attempt = 0; attempt < 100; attempt++)
            {
                if (broker.GetRetainedMessages().Count >= expected)
                {
                    return;
                }

                await Task.Delay(
                    TimeSpan.FromMilliseconds(50),
                    TestContext.Current.CancellationToken);
            }

            throw new TimeoutException("The retained message was not stored.");
        }
    }
}
