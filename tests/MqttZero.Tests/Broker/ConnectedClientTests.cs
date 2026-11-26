using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttZero.AspNetCore;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Observers;
using MqttZero.Protocol;
using MqttZero.Publishing;
using System.Buffers;
using System.Collections.Concurrent;
using System.Net;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task ConnectedClientHandleCanSubscribeAndSendPublish()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<ConnectedClientRecorder>();
            builder.Services.AddMqttZero(options =>
                {
                    options.MaxSubscriptionsPerSession = 1;
                })
                .AddConnectionObserver<HandleObserver>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            ConcurrentQueue<string> received = new();
            TaskCompletionSource<bool> messageReceived = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string?> contentType = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string?> userProperty = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            client.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                contentType.TrySetResult(eventArgs.ApplicationMessage.ContentType);
                userProperty.TrySetResult(eventArgs.ApplicationMessage.UserProperties is
                { Count: > 0 } properties
                        ? MQTTnet.Packets.MqttUserPropertyExtensions.ReadValueAsString(
                            properties[0])
                        : null);
                messageReceived.TrySetResult(true);
                return Task.CompletedTask;
            };
            await client.ConnectAsync(
                CreateClientOptions(port, "managed-client"),
                TestContext.Current.CancellationToken);

            ConnectedClientRecorder recorder =
                app.Services.GetRequiredService<ConnectedClientRecorder>();
            MqttZeroConnectedClient connected = await recorder.Client.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            await recorder.Subscribed.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);

            await connected.SendPublishAsync(
                new MqttZeroPublishMessage
                {
                    Topic = "managed/direct",
                    Payload = new ReadOnlySequence<byte>("direct"u8.ToArray()),
                    Properties = new MqttPublishProperties
                    {
                        ContentType = "text/plain",
                        UserProperties = [new MqttUserProperty("trace", "direct")]
                    }
                },
                TestContext.Current.CancellationToken);

            _ = await messageReceived.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            Assert.Equal(["direct"], received.ToArray());
            Assert.Equal("text/plain", await contentType.Task);
            Assert.Equal("direct", await userProperty.Task);
            Assert.Same(connected.Items, recorder.Items);

            IReadOnlyList<MqttZeroSubscribeResult> invalidSubscription =
                await connected.SubscribeAsync(
                    [new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("invalid/#/filter"u8.ToArray())
                    }],
                    cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.TopicFilterInvalid,
                Assert.Single(invalidSubscription).ReasonCode);
            IReadOnlyList<MqttZeroSubscribeResult> quota = await connected.SubscribeAsync(
                [new MqttSubscriptionRequest
                {
                    TopicFilter = new ReadOnlySequence<byte>("quota/#"u8.ToArray())
                }],
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.QuotaExceeded,
                Assert.Single(quota).ReasonCode);
            IReadOnlyList<MqttZeroSubscribeResult> replacedSubscription =
                await connected.SubscribeAsync(
                    [new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("managed/#"u8.ToArray()),
                        QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce,
                        RetainHandling = MqttRetainHandling.DoNotSendOnSubscribe
                    }],
                    cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.GrantedQos2,
                Assert.Single(replacedSubscription).ReasonCode);
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await connected.SendPublishAsync(
                    new MqttZeroPublishMessage
                    {
                        Topic = "invalid/+",
                        Payload = new ReadOnlySequence<byte>("value"u8.ToArray())
                    },
                    TestContext.Current.CancellationToken));
            IReadOnlyList<MqttZeroUnsubscribeResult> unsubscribeResults =
                await connected.UnsubscribeAsync(
                    ["managed/#"],
                    TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.Success,
                Assert.Single(unsubscribeResults).ReasonCode);
            IReadOnlyList<MqttZeroUnsubscribeResult> missingSubscription =
                await connected.UnsubscribeAsync(
                    ["managed/#"],
                    TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.NoSubscriptionExisted,
                Assert.Single(missingSubscription).ReasonCode);
            Assert.True(await connected.DisconnectAsync(
                cancellationToken: TestContext.Current.CancellationToken));
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task DisconnectedHandleRejectsStatefulOperations()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<ConnectedClientRecorder>();
            builder.Services.AddMqttZero().AddConnectionObserver<HandleObserver>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            await client.ConnectAsync(
                CreatePersistentClientOptions(port, "stale-handle"),
                TestContext.Current.CancellationToken);
            ConnectedClientRecorder recorder =
                app.Services.GetRequiredService<ConnectedClientRecorder>();
            MqttZeroConnectedClient connected = await recorder.Client.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            await client.DisconnectAsync(
                cancellationToken: TestContext.Current.CancellationToken);
            await WaitForSessionDisconnectedAsync(
                app.Services.GetRequiredService<MqttZeroBroker>(),
                "stale-handle",
                TestContext.Current.CancellationToken);

            Assert.False(connected.IsConnected);
            Assert.False(await connected.SendPublishAsync(
                new MqttZeroPublishMessage
                {
                    Topic = "stale/topic",
                    Payload = new ReadOnlySequence<byte>("value"u8.ToArray())
                },
                TestContext.Current.CancellationToken));
            Assert.False(await connected.DisconnectAsync(
                cancellationToken: TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await connected.SubscribeAsync(
                    [new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("stale/#"u8.ToArray())
                    }],
                    cancellationToken: TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await connected.UnsubscribeAsync(
                    ["stale/#"],
                    TestContext.Current.CancellationToken));
            Assert.Throws<InvalidOperationException>(() => connected.GetSnapshot());
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        private sealed class ConnectedClientRecorder
        {
            public TaskCompletionSource<MqttZeroConnectedClient> Client { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource<bool> Subscribed { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            public IDictionary<object, object?>? Items { get; set; }
        }

        private sealed class HandleObserver : IMqttZeroConnectionObserver
        {
            private readonly ConnectedClientRecorder _recorder;

            public HandleObserver(ConnectedClientRecorder recorder)
            {
                _recorder = recorder;
            }

            public async ValueTask OnConnectedAsync(
                MqttZeroClientConnectedContext context,
                CancellationToken cancellationToken)
            {
                _recorder.Items = context.ConnectedClient.Items;
                context.ConnectedClient.Items["role"] = "device";
                _recorder.Client.TrySetResult(context.ConnectedClient);
                await context.ConnectedClient.SubscribeAsync(
                    [new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("managed/#"u8.ToArray())
                    }],
                    cancellationToken: cancellationToken);
                _recorder.Subscribed.TrySetResult(true);
            }

            public ValueTask OnDisconnectedAsync(
                MqttZeroClientDisconnectedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
