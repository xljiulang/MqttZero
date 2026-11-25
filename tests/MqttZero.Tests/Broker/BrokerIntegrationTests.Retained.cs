using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttQualityOfServiceLevel = MqttZero.Protocol.MqttQualityOfServiceLevel;
using System.Buffers;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Protocol;
using MqttZero.AspNetCore;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 15_000)]
        public async Task ReplaysRetainedMessageToNewSubscriber()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero());
            });
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await publisher.ConnectAsync(CreateClientOptions(port, "retained-publisher"), cancellationToken);
            await publisher.PublishStringAsync(
                "retained/value",
                "saved",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
                retain: true,
                cancellationToken: cancellationToken);

            await subscriber.ConnectAsync(CreateClientOptions(port, "retained-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "retained/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            Assert.Equal("saved", await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 15_000)]
        public async Task DoesNotReplayExpiredRetainedMessage()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<bool> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(true);
                return Task.CompletedTask;
            };

            await publisher.ConnectAsync(CreateClientOptions(port, "retained-expiry-publisher"), cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("retained/expiry")
                    .WithPayload("temporary")
                    .WithRetainFlag()
                    .WithMessageExpiryInterval(1)
                    .Build(),
                cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            Assert.Empty(broker.GetRetainedMessages());
            List<MqttZero.Server.Retained.MqttZeroRetainedMessage> matches = [];
            broker.RetainedTopicTrieForTest.Match(
                "retained/#"u8,
                matches);
            Assert.Empty(matches);

            await subscriber.ConnectAsync(CreateClientOptions(port, "retained-expiry-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("retained/#")
                    .Build(),
                cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            Assert.False(received.Task.IsCompleted);

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
    }
}
