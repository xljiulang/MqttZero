using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttZero.AspNetCore;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using System.Collections.Concurrent;
using System.Net;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task InboundWorkFailureKeepsTheBrokerRunning()
        {
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(
                    IPAddress.Loopback,
                    port,
                    listen => listen.UseMqttZero()));
            builder.Services
                .AddMqttZero()
                .AddEnqueueInterceptor<ThrowingBoomInterceptor>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(cancellationToken);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            ConcurrentQueue<string> received = new();
            TaskCompletionSource<string> survivorMessage = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                string payload =
                    eventArgs.ApplicationMessage.ConvertPayloadToString();
                received.Enqueue(payload);
                survivorMessage.TrySetResult(payload);
                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "isolation-subscriber"),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("isolation/#")
                    .Build(),
                cancellationToken);

            using IMqttClient failing = factory.CreateMqttClient();
            await failing.ConnectAsync(
                CreateClientOptions(port, "isolation-failing"),
                cancellationToken);
            await failing.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("isolation/boom")
                    .WithPayload("boom")
                    .WithQualityOfServiceLevel(
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce)
                    .Build(),
                cancellationToken);

            Assert.Equal(MqttZeroBrokerState.Running, broker.State);

            using IMqttClient failingQosOne = factory.CreateMqttClient();
            await failingQosOne.ConnectAsync(
                CreateClientOptions(port, "isolation-failing-qos1"),
                cancellationToken);
            try
            {
                await failingQosOne.PublishAsync(
                    new MqttApplicationMessageBuilder()
                        .WithTopic("isolation/boom")
                        .WithPayload("boom")
                        .WithQualityOfServiceLevel(
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
            }
            catch (MQTTnet.Exceptions.MqttClientDisconnectedException)
            {
            }

            Assert.Equal(MqttZeroBrokerState.Running, broker.State);

            using IMqttClient survivor = factory.CreateMqttClient();
            await survivor.ConnectAsync(
                CreateClientOptions(port, "isolation-survivor"),
                cancellationToken);
            await survivor.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("isolation/fine")
                    .WithPayload("fine")
                    .WithQualityOfServiceLevel(
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);
            Assert.Equal(
                "fine",
                await survivorMessage.Task.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    cancellationToken));
            Assert.Equal(MqttZeroBrokerState.Running, broker.State);
            await app.StopAsync(cancellationToken);
        }

        private sealed class ThrowingBoomInterceptor
            : IMqttZeroEnqueueInterceptor
        {
            public ValueTask<MqttZeroEnqueueInterceptionResult> InterceptAsync(
                MqttZeroEnqueueContext context,
                CancellationToken cancellationToken)
            {
                if (context.Topic.Length > 0 &&
                    context.Topic.FirstSpan.EndsWith("boom"u8))
                {
                    throw new InvalidOperationException(
                        "The test interceptor rejects the payload.");
                }

                return ValueTask.FromResult(
                    MqttZeroEnqueueInterceptionResult.Enqueue);
            }
        }
    }
}
