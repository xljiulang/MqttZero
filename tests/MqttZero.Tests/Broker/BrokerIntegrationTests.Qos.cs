using MqttZero.Server;
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
        [Fact(Timeout = 30_000)]
        public async Task RoutesQos2MessageExactlyOnce()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            System.Collections.Concurrent.ConcurrentQueue<string> received = new();
            TaskCompletionSource<bool> firstReceived = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                firstReceived.TrySetResult(true);
                return Task.CompletedTask;
            };

            await subscriber.ConnectAsync(
                CreatePersistentClientOptions(port, "qos2-subscriber"),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "qos2/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce)
                    .Build(),
                cancellationToken);
            await publisher.ConnectAsync(
                CreateClientOptions(port, "qos2-publisher"),
                cancellationToken);
            await publisher.PublishStringAsync(
                "qos2/value",
                "exactly-once",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
                cancellationToken: cancellationToken);

            _ = await firstReceived.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            await Task.Delay(
                TimeSpan.FromMilliseconds(250),
                cancellationToken);
            Assert.Equal(["exactly-once"], received.ToArray());

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task DeduplicatesInboundQos2PublishUntilPubRel()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            System.Collections.Concurrent.ConcurrentQueue<string> received = new();
            TaskCompletionSource<bool> delivered = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                delivered.TrySetResult(true);
                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "qos2-dedup-subscriber"),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("qos2-dedup/#")
                    .Build(),
                cancellationToken);

            using TcpClient publisher = new();
            await publisher.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = publisher.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("qos2-dedup-publisher"u8.ToArray()),
                    CleanStart = true
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));
            MqttPublishPacket publish = new()
            {
                TopicName = new ReadOnlySequence<byte>("qos2-dedup/value"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>("once"u8.ToArray()),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce,
                PacketIdentifier = 77
            };
            await SendPacketAsync(stream, publish, cancellationToken);
            _ = Assert.IsType<MqttPubRecPacket>(await ReadPacketAsync(stream, cancellationToken));
            publish.Duplicate = true;
            await SendPacketAsync(stream, publish, cancellationToken);
            _ = Assert.IsType<MqttPubRecPacket>(await ReadPacketAsync(stream, cancellationToken));
            await SendPacketAsync(
                stream,
                new MqttPubRelPacket
                {
                    PacketIdentifier = 77
                },
                cancellationToken);
            _ = Assert.IsType<MqttPubCompPacket>(await ReadPacketAsync(stream, cancellationToken));
            _ = await delivered.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            Assert.Equal(["once"], received.ToArray());

            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            publisher.Client.Shutdown(SocketShutdown.Both);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task ExpiredInboundQos2CompletesHandshakeWithoutRoutingOrRetaining()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<bool> received = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(true);
                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "qos2-expiry-subscriber"),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("qos2-expiry/#")
                    .Build(),
                cancellationToken);

            using TcpClient publisher = new();
            await publisher.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = publisher.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("qos2-expiry-publisher"u8.ToArray()),
                    CleanStart = true
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, cancellationToken));
            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicName = new ReadOnlySequence<byte>("qos2-expiry/value"u8.ToArray()),
                    Payload = new ReadOnlySequence<byte>("expired"u8.ToArray()),
                    QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce,
                    PacketIdentifier = 88,
                    Retain = true,
                    MessageExpiryInterval = 1
                },
                cancellationToken);
            _ = Assert.IsType<MqttPubRecPacket>(
                await ReadPacketAsync(stream, cancellationToken));
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            await SendPacketAsync(
                stream,
                new MqttPubRelPacket
                {
                    PacketIdentifier = 88
                },
                cancellationToken);
            _ = Assert.IsType<MqttPubCompPacket>(
                await ReadPacketAsync(stream, cancellationToken));
            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);

            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            Assert.False(received.Task.IsCompleted);
            Assert.Empty(broker.GetRetainedMessages());
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task QueuesQos1MessageWhileSessionOffline()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(CreatePersistentClientOptions(port, "offline-client"), cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "offline/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }
            await WaitForSessionDisconnectedAsync(broker, "offline-client", cancellationToken);

            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(CreateClientOptions(port, "offline-publisher"), cancellationToken);
            await publisher.PublishStringAsync(
                "offline/value",
                "queued",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            using IMqttClient restored = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            restored.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            MQTTnet.MqttClientConnectResult result =
                await restored.ConnectAsync(CreatePersistentClientOptions(port, "offline-client"), cancellationToken);

            Assert.True(result.IsSessionPresent);
            Assert.Equal(
                "queued",
                await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 60_000)]
        public async Task ResendsInflightQos1MessageWithDuplicateFlag()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            ushort originalPacketIdentifier;

            using (TcpClient first = new())
            {
                await first.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
                NetworkStream stream = first.GetStream();
                await SendPacketAsync(
                    stream,
                    new MqttConnectPacket
                    {
                        ClientId = new ReadOnlySequence<byte>("dup-client"u8.ToArray()),
                        CleanStart = false,
                        KeepAliveSeconds = 30,
                        SessionExpiryInterval = 60
                    },
                    cancellationToken);
                _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));
                await SendPacketAsync(
                    stream,
                    new MqttSubscribePacket
                    {
                        PacketIdentifier = 1,
                        Subscriptions =
                        [
                            new MqttSubscriptionRequest
                            {
                                TopicFilter = new ReadOnlySequence<byte>("dup/#"u8.ToArray()),
                                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                            }
                        ]
                    },
                    cancellationToken);
                _ = Assert.IsType<MqttSubAckPacket>(await ReadPacketAsync(stream, cancellationToken));

                await publisher.ConnectAsync(CreateClientOptions(port, "dup-publisher"), cancellationToken);
                await publisher.PublishStringAsync(
                    "dup/value",
                    "inflight",
                    MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                    cancellationToken: cancellationToken);

                MqttPublishPacket firstPublish = Assert.IsType<MqttPublishPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.False(firstPublish.Duplicate);
                originalPacketIdentifier = firstPublish.PacketIdentifier;
                first.Client.LingerState = new LingerOption(true, 0);
                first.Client.Shutdown(SocketShutdown.Both);
                first.Close();
            }

            using (TcpClient restored = new())
            {
                await restored.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
                NetworkStream stream = restored.GetStream();
                await SendPacketAsync(
                    stream,
                    new MqttConnectPacket
                    {
                        ClientId = new ReadOnlySequence<byte>("dup-client"u8.ToArray()),
                        CleanStart = false,
                        KeepAliveSeconds = 30,
                        SessionExpiryInterval = 60
                    },
                    cancellationToken);
                MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.True(connAck.SessionPresent);

                MqttPublishPacket resentPublish = Assert.IsType<MqttPublishPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.True(resentPublish.Duplicate);
                Assert.Equal(originalPacketIdentifier, resentPublish.PacketIdentifier);
                Assert.Equal(
                    "inflight",
                    System.Text.Encoding.UTF8.GetString(resentPublish.Payload.FirstSpan));
                await SendPacketAsync(
                    stream,
                    new MqttPubAckPacket
                    {
                        PacketIdentifier = originalPacketIdentifier
                    },
                    cancellationToken);
                restored.Client.Shutdown(SocketShutdown.Both);
            }

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task DropsExpiredQueuedMessageBeforeReconnectDelivery()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(CreatePersistentClientOptions(port, "expiry-client"), cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "expiry/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }

            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(CreateClientOptions(port, "expiry-publisher"), cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("expiry/value")
                    .WithPayload("temporary")
                    .WithQualityOfServiceLevel(
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithMessageExpiryInterval(1)
                    .Build(),
                cancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            using IMqttClient restored = factory.CreateMqttClient();
            TaskCompletionSource<bool> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            restored.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(true);
                return Task.CompletedTask;
            };
            MQTTnet.MqttClientConnectResult result =
                await restored.ConnectAsync(CreatePersistentClientOptions(port, "expiry-client"), cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);

            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttZeroSessionSnapshot snapshot = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "expiry-client");
            Assert.True(result.IsSessionPresent);
            Assert.False(received.Task.IsCompleted);
            Assert.Equal(0, snapshot.PendingMessageCount);

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task EvictsOldestMessagesWhenSessionQueueIsFull()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options => options.MaxMessageQueueLength = 2);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(CreatePersistentClientOptions(port, "queue-client"), cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "queue/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }
            await WaitForSessionDisconnectedAsync(broker, "queue-client", cancellationToken);

            for (int index = 1; index <= 3; index++)
            {
                await broker.PublishAsync(
                    "queue/value",
                    new ReadOnlySequence<byte>(
                        System.Text.Encoding.UTF8.GetBytes(
                            index.ToString(CultureInfo.InvariantCulture))),
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    false,
                    cancellationToken);
            }

            MqttZeroSessionSnapshot queuedSession = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "queue-client");
            Assert.Equal(2, queuedSession.PendingMessageCount);

            using IMqttClient restored = factory.CreateMqttClient();
            System.Collections.Concurrent.ConcurrentQueue<string> payloads = new();
            TaskCompletionSource<bool> receivedBoth = new(TaskCreationOptions.RunContinuationsAsynchronously);
            restored.ApplicationMessageReceivedAsync += eventArgs =>
            {
                payloads.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                if (payloads.Count == 2)
                {
                    receivedBoth.TrySetResult(true);
                }

                return Task.CompletedTask;
            };
            _ = await restored.ConnectAsync(CreatePersistentClientOptions(port, "queue-client"), cancellationToken);
            _ = await receivedBoth.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
            Assert.Equal(["2", "3"], payloads.ToArray());

            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
    }
}
