using MqttZero.Protocol;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttZero.AspNetCore;
using MqttZero.Publishing;
using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task QueuedPersistenceStoresPropertiesAndAllSubscriptionIdentifiers()
        {
            int port = GetFreePort();
            ControllableMessageStore messageStore = new();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            builder.Services.AddMqttZero();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();

            using (IMqttClient subscriber = factory.CreateMqttClient())
            {
                await subscriber.ConnectAsync(
                    CreatePersistentClientOptions(port, "persisted-properties"),
                    cancellationToken);
                await subscriber.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "persisted/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithSubscriptionIdentifier(17)
                        .Build(),
                    cancellationToken);
                await subscriber.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "persisted/+",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .WithSubscriptionIdentifier(23)
                        .Build(),
                    cancellationToken);
                await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            }

            await WaitForSessionDisconnectedAsync(
                broker,
                "persisted-properties",
                cancellationToken);
            await broker.PublishAsync(
                new MqttZeroPublishMessage
                {
                    Topic = "persisted/value",
                    Payload = new ReadOnlySequence<byte>("queued"u8.ToArray()),
                    QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                    Properties = new MqttPublishProperties
                    {
                        ContentType = "text/plain",
                        UserProperties = [new MqttUserProperty("trace", "persisted")]
                    }
                },
                cancellationToken);

            MqttZeroPersistedMessage persisted =
                Assert.Single(messageStore.Messages.Values);
            Assert.Equal(new uint[] { 17, 23 }, persisted.SubscriptionIdentifiers.ToArray());
            Assert.True(MqttPublishPropertiesCodec.TryDecode(
                new ReadOnlySequence<byte>(persisted.Properties),
                out MqttPublishProperties properties,
                out _,
                out _));
            Assert.Equal("text/plain", properties.ContentType);
            Assert.Equal("persisted", Assert.Single(properties.UserProperties).Value);
        }

        [Fact(Timeout = 30_000)]
        public async Task QueuedTransitionFailureRestoresPendingMessage()
        {
            int port = GetFreePort();
            ControllableMessageStore messageStore = new();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            builder.Services.AddMqttZero();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(
                    CreatePersistentClientOptions(port, "transition-client"),
                    cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "transition/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }

            await WaitForSessionDisconnectedAsync(
                broker,
                "transition-client",
                cancellationToken);
            await broker.PublishAsync(
                "transition/value",
                new ReadOnlySequence<byte>("queued"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                cancellationToken);
            messageStore.FailTransition = true;

            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("transition-client"u8.ToArray()),
                    CleanStart = false,
                    SessionExpiryInterval = 60
                },
                cancellationToken);
            MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, cancellationToken));
            Assert.True(connAck.SessionPresent);
            using CancellationTokenSource closeTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            closeTimeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] buffer = new byte[1];
            Assert.Equal(0, await stream.ReadAsync(buffer, closeTimeout.Token));

            MqttZeroSessionSnapshot session = Assert.Single(
                broker.GetSessions(),
                item => item.ClientId == "transition-client");
            Assert.Equal(1, session.PendingMessageCount);
            Assert.Equal(0, session.InflightMessageCount);
            Assert.Single(messageStore.Messages.Values);
            Assert.Equal(
                MqttZeroPersistedMessageKind.Queued,
                messageStore.Messages.Values.Single().Kind);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task InboundQos2DeleteFailureDoesNotRouteAgainOnRetry()
        {
            int port = GetFreePort();
            ControllableMessageStore messageStore = new();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            builder.Services.AddMqttZero();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            ConcurrentQueue<string> received = new();
            TaskCompletionSource<bool> delivered = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                delivered.TrySetResult(true);
                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "delete-failure-subscriber"),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "delete-failure/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            using (TcpClient publisher = new())
            {
                await publisher.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
                NetworkStream stream = publisher.GetStream();
                await SendPacketAsync(
                    stream,
                    new MqttConnectPacket
                    {
                        ClientId = new ReadOnlySequence<byte>(
                            "delete-failure-publisher"u8.ToArray()),
                        CleanStart = false,
                        SessionExpiryInterval = 60
                    },
                    cancellationToken);
                MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.False(connAck.SessionPresent);
                await SendPacketAsync(
                    stream,
                    new MqttPublishPacket
                    {
                        TopicName = new ReadOnlySequence<byte>(
                            "delete-failure/value"u8.ToArray()),
                        Payload = new ReadOnlySequence<byte>("once"u8.ToArray()),
                        QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce,
                        PacketIdentifier = 77
                    },
                    cancellationToken);
                _ = Assert.IsType<MqttPubRecPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                messageStore.FailDeleteInbound = true;
                await SendPacketAsync(
                    stream,
                    new MqttPubRelPacket
                    {
                        PacketIdentifier = 77
                    },
                    cancellationToken);
                _ = await delivered.Task.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    cancellationToken);
                using CancellationTokenSource closeTimeout =
                    CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                closeTimeout.CancelAfter(TimeSpan.FromSeconds(2));
                byte[] buffer = new byte[1];
                Assert.Equal(0, await stream.ReadAsync(buffer, closeTimeout.Token));
            }

            messageStore.FailDeleteInbound = false;
            using (TcpClient retry = new())
            {
                await retry.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
                NetworkStream stream = retry.GetStream();
                await SendPacketAsync(
                    stream,
                    new MqttConnectPacket
                    {
                        ClientId = new ReadOnlySequence<byte>(
                            "delete-failure-publisher"u8.ToArray()),
                        CleanStart = false,
                        SessionExpiryInterval = 60
                    },
                    cancellationToken);
                MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.True(connAck.SessionPresent);
                await SendPacketAsync(
                    stream,
                    new MqttPubRelPacket
                    {
                        PacketIdentifier = 77
                    },
                    cancellationToken);
                MqttPubCompPacket completion = Assert.IsType<MqttPubCompPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.Equal(MqttReasonCode.Success, completion.ReasonCode);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            Assert.Equal(["once"], received.ToArray());
            Assert.DoesNotContain(
                messageStore.Messages.Values,
                message => message.Kind == MqttZeroPersistedMessageKind.InboundQos2);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task DueWillRetriesAfterPublishFailure()
        {
            int port = GetFreePort();
            FailOnceWillInterceptor interceptor = new();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroPublishInterceptor>(interceptor);
            builder.Services.AddMqttZero();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient watcher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            watcher.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            await watcher.ConnectAsync(
                CreateClientOptions(port, "will-retry-watcher"),
                cancellationToken);
            await watcher.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "will/retry",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("will-retry-client"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 1,
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/retry"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("retried"u8.ToArray()),
                    WillQualityOfServiceLevel =
                        MqttQualityOfServiceLevel.AtLeastOnce
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, cancellationToken));

            Assert.Equal(
                "retried",
                await received.Task.WaitAsync(
                    TimeSpan.FromSeconds(20),
                    cancellationToken));
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            await Task.Delay(TimeSpan.FromMilliseconds(750), cancellationToken);
            Assert.Equal(1, broker.PublishedWillCount);
            Assert.True(interceptor.CallCount >= 2);
            await watcher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task ExpiredQueueDeleteFailureRestoresMessageAndTimerRetries()
        {
            int port = GetFreePort();
            ControllableMessageStore messageStore = new();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            builder.Services.AddMqttZero();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(
                    CreatePersistentClientOptions(port, "expired-delete-client"),
                    cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "expired-delete/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }

            await WaitForSessionDisconnectedAsync(
                broker,
                "expired-delete-client",
                cancellationToken);
            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(
                CreateClientOptions(port, "expired-delete-publisher"),
                cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("expired-delete/value")
                    .WithPayload("expired")
                    .WithQualityOfServiceLevel(
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithMessageExpiryInterval(1)
                    .Build(),
                cancellationToken);
            await WaitUntilAsync(
                () => messageStore.Messages.Count == 1 &&
                    messageStore.Messages.Values.Single().Kind ==
                        MqttZeroPersistedMessageKind.Queued,
                cancellationToken);
            messageStore.FailNextDeleteMessage = true;
            await WaitUntilAsync(
                () => messageStore.DeleteAttempts >= 2 &&
                    messageStore.Messages.Count == 0 &&
                    broker.GetSessions().Any(
                        static session =>
                            session.ClientId == "expired-delete-client" &&
                            session.PendingMessageCount == 0),
                cancellationToken);

            MqttZeroSessionSnapshot restored = Assert.Single(
                broker.GetSessions(),
                item => item.ClientId == "expired-delete-client");
            Assert.Equal(0, restored.PendingMessageCount);
            Assert.Empty(messageStore.Messages.Values);
            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        private sealed class ControllableMessageStore : IMqttZeroMessageStore
        {
            private int _failTransition;
            private int _failDeleteInbound;
            private int _failNextDeleteMessage;
            private int _deleteAttempts;

            public bool FailTransition
            {
                get
                {
                    return Volatile.Read(ref _failTransition) != 0;
                }
                set
                {
                    Volatile.Write(ref _failTransition, value ? 1 : 0);
                }
            }

            public bool FailDeleteInbound
            {
                get
                {
                    return Volatile.Read(ref _failDeleteInbound) != 0;
                }
                set
                {
                    Volatile.Write(ref _failDeleteInbound, value ? 1 : 0);
                }
            }

            public bool FailNextDeleteMessage
            {
                get
                {
                    return Volatile.Read(ref _failNextDeleteMessage) != 0;
                }
                set
                {
                    Volatile.Write(ref _failNextDeleteMessage, value ? 1 : 0);
                }
            }

            public int DeleteAttempts
            {
                get
                {
                    return Volatile.Read(ref _deleteAttempts);
                }
            }

            public Dictionary<long, MqttZeroPersistedMessage> Messages { get; } = [];

            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedMessage>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedMessage>>(
                    Messages.Values.ToArray());
            }

            public ValueTask SaveQueuedAsync(
                MqttZeroPersistedMessage message,
                CancellationToken cancellationToken)
            {
                Messages[message.MessageId] = message;
                return ValueTask.CompletedTask;
            }

            public ValueTask SaveOutboundAsync(
                MqttZeroPersistedMessage message,
                CancellationToken cancellationToken)
            {
                Messages[message.MessageId] = message;
                return ValueTask.CompletedTask;
            }

            public ValueTask SaveInboundQos2Async(
                MqttZeroPersistedMessage message,
                CancellationToken cancellationToken)
            {
                Messages[message.MessageId] = message;
                return ValueTask.CompletedTask;
            }

            public ValueTask TransitionQueuedToOutboundAsync(
                string clientId,
                long messageId,
                MqttZeroPersistedMessage message,
                CancellationToken cancellationToken)
            {
                if (FailTransition)
                {
                    throw new InvalidOperationException("Transition failed.");
                }

                Messages.Remove(messageId);
                Messages[message.MessageId] = message;
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteMessageAsync(
                string clientId,
                long messageId,
                CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _deleteAttempts);
                if (Interlocked.Exchange(ref _failNextDeleteMessage, 0) != 0)
                {
                    throw new InvalidOperationException("Message delete failed.");
                }

                Messages.Remove(messageId);
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteInboundQos2Async(
                string clientId,
                ushort packetIdentifier,
                CancellationToken cancellationToken)
            {
                if (FailDeleteInbound)
                {
                    throw new InvalidOperationException("Delete failed.");
                }

                Messages.Remove(packetIdentifier);
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteByClientAsync(
                string clientId,
                CancellationToken cancellationToken)
            {
                foreach (long messageId in Messages
                    .Where(pair => pair.Value.ClientId == clientId)
                    .Select(pair => pair.Key)
                    .ToArray())
                {
                    Messages.Remove(messageId);
                }

                return ValueTask.CompletedTask;
            }

            public ValueTask ReplaceQueuedAsync(
                string clientId,
                long replacedMessageId,
                MqttZeroPersistedMessage message,
                CancellationToken cancellationToken)
            {
                Messages.Remove(replacedMessageId);
                Messages[message.MessageId] = message;
                return ValueTask.CompletedTask;
            }
        }

        private sealed class FailOnceWillInterceptor : IMqttZeroPublishInterceptor
        {
            private int _callCount;

            public int CallCount
            {
                get
                {
                    return Volatile.Read(ref _callCount);
                }
            }

            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroPublishContext context,
                CancellationToken cancellationToken)
            {
                if (context.Topic.ToArray().AsSpan().SequenceEqual("will/retry"u8))
                {
                    int call = Interlocked.Increment(ref _callCount);
                    if (call == 1)
                    {
                        throw new InvalidOperationException("First Will publish failed.");
                    }
                }

                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }

        private static async Task AssertPersistentReconnectClosesAsync(
            int port,
            string clientId,
            CancellationToken cancellationToken)
        {
            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>(
                        System.Text.Encoding.UTF8.GetBytes(clientId)),
                    CleanStart = false,
                    SessionExpiryInterval = 60
                },
                cancellationToken);
            MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, cancellationToken));
            Assert.True(connAck.SessionPresent);
            using CancellationTokenSource closeTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            closeTimeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] buffer = new byte[1];
            Assert.Equal(0, await stream.ReadAsync(buffer, closeTimeout.Token));
        }
    }
}
