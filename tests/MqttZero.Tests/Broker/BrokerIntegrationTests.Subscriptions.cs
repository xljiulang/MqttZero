using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
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
        public async Task FailedSubscriptionPersistenceDoesNotPublishRouteEpoch()
        {
            int port = GetFreePort();
            ControllableSessionStore sessionStore = new();
            await using WebApplication app = await StartStoredBrokerAsync(
                port,
                sessionStore);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            await client.ConnectAsync(
                CreatePersistentClientOptions(port, "failed-subscription"),
                TestContext.Current.CancellationToken);
            Assert.True(broker.TryGetClient("failed-subscription", out var connected));
            MqttZeroRouteEpoch baseline = broker.RouteEpoch;
            sessionStore.FailNextSave = true;

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await connected!.SubscribeAsync(
                    [
                        new MqttSubscriptionRequest
                        {
                            TopicFilter = new ReadOnlySequence<byte>(
                                "failure/#"u8.ToArray()),
                            QualityOfServiceLevel =
                                MqttQualityOfServiceLevel.AtLeastOnce
                        }
                    ],
                    cancellationToken:
                        TestContext.Current.CancellationToken).AsTask());

            MqttZeroSessionSnapshot session = Assert.Single(
                broker.GetSessions(),
                item => item.ClientId == "failed-subscription");
            Assert.Equal(0, session.SubscriptionCount);
            Assert.Same(baseline, broker.RouteEpoch);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task SuccessfulSubscriptionPersistencePublishesRouteEpoch()
        {
            int port = GetFreePort();
            ControllableSessionStore sessionStore = new();
            await using WebApplication app = await StartStoredBrokerAsync(
                port,
                sessionStore);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            await client.ConnectAsync(
                CreatePersistentClientOptions(port, "saved-subscription"),
                TestContext.Current.CancellationToken);
            Assert.True(broker.TryGetClient("saved-subscription", out var connected));
            MqttZeroRouteEpoch baseline = broker.RouteEpoch;

            IReadOnlyList<MqttZeroSubscribeResult> results =
                await connected!.SubscribeAsync(
                    [
                        new MqttSubscriptionRequest
                        {
                            TopicFilter = new ReadOnlySequence<byte>(
                                "success/#"u8.ToArray()),
                            QualityOfServiceLevel =
                                MqttQualityOfServiceLevel.AtLeastOnce
                        }
                    ],
                    cancellationToken:
                        TestContext.Current.CancellationToken);

            Assert.Single(results);
            Assert.Single(
                sessionStore.Sessions["saved-subscription"].Subscriptions);
            Assert.NotSame(baseline, broker.RouteEpoch);
            Assert.Equal(baseline.Version + 1, broker.RouteEpoch.Version);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task FailedUnsubscribePersistenceKeepsRouteEpochAndSubscription()
        {
            int port = GetFreePort();
            ControllableSessionStore sessionStore = new();
            await using WebApplication app = await StartStoredBrokerAsync(
                port,
                sessionStore);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            await client.ConnectAsync(
                CreatePersistentClientOptions(port, "failed-unsubscribe"),
                TestContext.Current.CancellationToken);
            Assert.True(broker.TryGetClient("failed-unsubscribe", out var connected));
            await connected!.SubscribeAsync(
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>(
                            "persistent/#"u8.ToArray()),
                        QualityOfServiceLevel =
                            MqttQualityOfServiceLevel.AtLeastOnce
                    }
                ],
                cancellationToken:
                    TestContext.Current.CancellationToken);
            MqttZeroRouteEpoch baseline = broker.RouteEpoch;
            sessionStore.FailNextSave = true;

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await connected.UnsubscribeAsync(
                    ["persistent/#"],
                    TestContext.Current.CancellationToken).AsTask());

            MqttZeroSessionSnapshot session = Assert.Single(
                broker.GetSessions(),
                item => item.ClientId == "failed-unsubscribe");
            Assert.Equal(1, session.SubscriptionCount);
            Assert.Same(baseline, broker.RouteEpoch);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task FailedPacketSubscribePersistenceDoesNotCommitRoute()
        {
            int port = GetFreePort();
            ControllableSessionStore sessionStore = new();
            await using WebApplication app = await StartStoredBrokerAsync(
                port,
                sessionStore);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            using TcpClient client = new();
            await client.ConnectAsync(
                IPAddress.Loopback,
                port,
                TestContext.Current.CancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>(
                        "packet-failed-subscribe"u8.ToArray()),
                    CleanStart = false,
                    SessionExpiryInterval = 60
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(
                    stream,
                    TestContext.Current.CancellationToken));
            MqttZeroRouteEpoch baseline = broker.RouteEpoch;
            sessionStore.FailNextSave = true;

            await SendPacketAsync(
                stream,
                new MqttSubscribePacket
                {
                    PacketIdentifier = 1,
                    Subscriptions =
                    [
                        new MqttSubscriptionRequest
                        {
                            TopicFilter = new ReadOnlySequence<byte>(
                                "packet-failure/#"u8.ToArray()),
                            QualityOfServiceLevel =
                                MqttQualityOfServiceLevel.AtLeastOnce
                        }
                    ]
                },
                TestContext.Current.CancellationToken);
            using CancellationTokenSource timeout =
                CancellationTokenSource.CreateLinkedTokenSource(
                    TestContext.Current.CancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] buffer = new byte[1];
            Assert.Equal(0, await stream.ReadAsync(buffer, timeout.Token));

            MqttZeroSessionSnapshot session = Assert.Single(
                broker.GetSessions(),
                item => item.ClientId == "packet-failed-subscribe");
            Assert.Equal(0, session.SubscriptionCount);
            Assert.Same(baseline, broker.RouteEpoch);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task UnsubscribeRemovesPersistentSubscription()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            await client.ConnectAsync(CreateClientOptions(port, "unsubscribe-client"), cancellationToken);
            await client.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "remove/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);
            await client.UnsubscribeAsync(
                new MqttClientUnsubscribeOptionsBuilder()
                    .WithTopicFilter("remove/#")
                    .Build(),
                cancellationToken);

            MqttZeroSessionSnapshot snapshot = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "unsubscribe-client");
            Assert.Equal(0, snapshot.SubscriptionCount);

            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        private static async Task<WebApplication> StartStoredBrokerAsync(
            int port,
            IMqttZeroSessionStore sessionStore)
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(
                    IPAddress.Loopback,
                    port,
                    listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            builder.Services.AddMqttZero();
            WebApplication app = builder.Build();
            await app.StartAsync(
                TestContext.Current.CancellationToken);
            return app;
        }
        [Fact(Timeout = 15_000)]
        public async Task SharedSubscriptionSelectsOneSubscriberPerPublish()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient worker1 = factory.CreateMqttClient();
            using IMqttClient worker2 = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received1 = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string> received2 = new(TaskCreationOptions.RunContinuationsAsynchronously);
            worker1.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received1.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            worker2.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received2.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await worker1.ConnectAsync(CreateClientOptions(port, "worker-1"), cancellationToken);
            await worker2.ConnectAsync(CreateClientOptions(port, "worker-2"), cancellationToken);
            await publisher.ConnectAsync(CreateClientOptions(port, "shared-publisher"), cancellationToken);
            foreach (IMqttClient worker in new[] { worker1, worker2 })
            {
                await worker.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "$share/workers/jobs/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
            }

            await publisher.PublishStringAsync(
                "jobs/one",
                "first",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);
            await publisher.PublishStringAsync(
                "jobs/two",
                "second",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            string[] payloads =
            [
                await received1.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken),
                await received2.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken)
            ];
            Assert.Contains("first", payloads);
            Assert.Contains("second", payloads);

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await worker1.DisconnectAsync(cancellationToken: cancellationToken);
            await worker2.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 15_000)]
        public async Task ResolvesInboundTopicAlias()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            System.Collections.Concurrent.ConcurrentQueue<string> payloads = new();
            TaskCompletionSource<bool> receivedBoth = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                payloads.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                if (payloads.Count >= 2)
                {
                    receivedBoth.TrySetResult(true);
                }

                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(CreateClientOptions(port, "alias-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("alias/topic")
                    .Build(),
                cancellationToken);

            using TcpClient publisher = new();
            await publisher.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = publisher.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("alias-publisher"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 30
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));
            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicName = new ReadOnlySequence<byte>("alias/topic"u8.ToArray()),
                    TopicAlias = 1000,
                    Payload = new ReadOnlySequence<byte>("first"u8.ToArray())
                },
                cancellationToken);
            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicAlias = 1000,
                    Payload = new ReadOnlySequence<byte>("second"u8.ToArray())
                },
                cancellationToken);

            _ = await receivedBoth.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
            Assert.Equal(["first", "second"], payloads.ToArray());

            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            publisher.Client.Shutdown(SocketShutdown.Both);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 15_000)]
        public async Task AppliesNoLocalAndSubscriptionIdentifier()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient otherPublisher = factory.CreateMqttClient();
            System.Collections.Concurrent.ConcurrentQueue<string> payloads = new();
            System.Collections.Concurrent.ConcurrentQueue<uint[]> identifiers = new();
            TaskCompletionSource<bool> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                payloads.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                identifiers.Enqueue(
                    eventArgs.ApplicationMessage.SubscriptionIdentifiers?.ToArray() ?? []);
                received.TrySetResult(true);
                return Task.CompletedTask;
            };

            await subscriber.ConnectAsync(CreateClientOptions(port, "no-local-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        new MqttTopicFilterBuilder()
                            .WithTopic("no-local/topic")
                            .WithNoLocal()
                            .Build())
                    .WithSubscriptionIdentifier(77)
                    .Build(),
                cancellationToken);
            await subscriber.PublishStringAsync(
                "no-local/topic",
                "self",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            await otherPublisher.ConnectAsync(CreateClientOptions(port, "other-publisher"), cancellationToken);
            await otherPublisher.PublishStringAsync(
                "no-local/topic",
                "other",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            _ = await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
            Assert.Equal(["other"], payloads.ToArray());
            Assert.Equal([77U], identifiers.ToArray()[0]);

            await otherPublisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task IncludesAllMatchingSubscriptionIdentifiersInSinglePublish()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<uint[]> identifiers = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            int deliveryCount = 0;
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                Interlocked.Increment(ref deliveryCount);
                identifiers.TrySetResult(
                    eventArgs.ApplicationMessage.SubscriptionIdentifiers?.ToArray() ?? []);
                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "overlapping-subscriber"),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("overlap/#")
                    .WithSubscriptionIdentifier(11)
                    .Build(),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("overlap/+")
                    .WithSubscriptionIdentifier(22)
                    .Build(),
                cancellationToken);
            await publisher.ConnectAsync(
                CreateClientOptions(port, "overlapping-publisher"),
                cancellationToken);

            await publisher.PublishStringAsync(
                "overlap/value",
                "payload",
                cancellationToken: cancellationToken);

            uint[] received = await identifiers.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            Assert.Equal([11U, 22U], received);
            Assert.Equal(1, deliveryCount);

            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 15_000)]
        public async Task HonorsRetainHandlingDoNotSendOnSubscribe()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await publisher.ConnectAsync(CreateClientOptions(port, "retain-handling-publisher"), cancellationToken);
            await publisher.PublishStringAsync(
                "retain-handling/value",
                "retained",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                retain: true,
                cancellationToken: cancellationToken);

            await subscriber.ConnectAsync(CreateClientOptions(port, "retain-handling-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        new MqttTopicFilterBuilder()
                            .WithTopic("retain-handling/#")
                            .WithRetainHandling(MQTTnet.Protocol.MqttRetainHandling.DoNotSendOnSubscribe)
                            .Build())
                    .Build(),
                cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            Assert.False(received.Task.IsCompleted);

            await publisher.PublishStringAsync(
                "retain-handling/value",
                "live",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);
            Assert.Equal(
                "live",
                await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
    }
}
