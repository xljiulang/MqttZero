using MqttZero.Server;
using MqttZero.Server.Broker;
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
        [Fact(Timeout = 30_000)]
        public async Task RestoresPersistentSessionAndSubscription()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(CreatePersistentClientOptions(port, "persistent-client"), cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "persistent/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }

            using IMqttClient restored = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            restored.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            MQTTnet.MqttClientConnectResult result =
                await restored.ConnectAsync(CreatePersistentClientOptions(port, "persistent-client"), cancellationToken);
            await publisher.ConnectAsync(CreateClientOptions(port, "persistent-publisher"), cancellationToken);

            Assert.True(result.IsSessionPresent);

            await publisher.PublishStringAsync(
                "persistent/value",
                "restored",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            Assert.Equal(
                "restored",
                await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task ExpiresDisconnectedSession()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            await client.ConnectAsync(
                new MqttClientOptionsBuilder()
                    .WithTcpServer("127.0.0.1", port)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                    .WithClientId("expiring-client")
                    .WithCleanStart(false)
                    .WithSessionExpiryInterval(1)
                    .Build(),
                cancellationToken);
            await client.DisconnectAsync(cancellationToken: cancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            Assert.DoesNotContain(
                broker.GetSessions(),
                session => session.ClientId == "expiring-client");
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task TakesOverExistingClientId()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient first = factory.CreateMqttClient();
            using IMqttClient second = factory.CreateMqttClient();
            TaskCompletionSource<MQTTnet.MqttClientDisconnectReason> firstDisconnected =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            first.DisconnectedAsync += eventArgs =>
            {
                firstDisconnected.TrySetResult(eventArgs.Reason);
                return Task.CompletedTask;
            };

            await first.ConnectAsync(CreatePersistentClientOptions(port, "takeover-client"), cancellationToken);
            MQTTnet.MqttClientConnectResult result =
                await second.ConnectAsync(CreatePersistentClientOptions(port, "takeover-client"), cancellationToken);

            Assert.True(result.IsSessionPresent);
            Assert.Equal(
                MQTTnet.MqttClientDisconnectReason.SessionTakenOver,
                await firstDisconnected.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await second.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task ClampsSessionExpiryInConnAck()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options => options.MaxSessionExpiryInterval = TimeSpan.FromSeconds(1));
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            MQTTnet.MqttClientConnectResult result = await client.ConnectAsync(
                new MqttClientOptionsBuilder()
                    .WithTcpServer("127.0.0.1", port)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                    .WithClientId("clamped-client")
                    .WithCleanStart(false)
                    .WithSessionExpiryInterval(60)
                    .Build(),
                cancellationToken);

            Assert.Equal(1U, result.SessionExpiryInterval);
            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task DoesNotClampSessionExpiryByDefault()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            MQTTnet.MqttClientConnectResult result = await client.ConnectAsync(
                new MqttClientOptionsBuilder()
                    .WithTcpServer("127.0.0.1", port)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                    .WithClientId("unclamped-client")
                    .WithCleanStart(false)
                    .WithSessionExpiryInterval(86_400)
                    .Build(),
                cancellationToken);

            Assert.Equal(86_400U, result.SessionExpiryInterval);
            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task DisconnectCanOverrideSessionExpiryToZero()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("disconnect-expiry"u8.ToArray()),
                    CleanStart = false,
                    KeepAliveSeconds = 30,
                    SessionExpiryInterval = 60
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));
            await SendPacketAsync(
                stream,
                new MqttDisconnectPacket
                {
                    SessionExpiryInterval = 0
                },
                cancellationToken);
            using CancellationTokenSource closeTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            closeTimeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] endOfStream = new byte[1];
            _ = await stream.ReadAsync(endOfStream, closeTimeout.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);

            Assert.DoesNotContain(
                broker.GetSessions(),
                session => session.ClientId == "disconnect-expiry");
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task PersistsWillAndDisconnectedTimestamp()
        {
            int port = GetFreePort();
            RecordingSessionStore sessionStore = new();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            builder.Services.AddMqttZero();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);

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
                    ClientId = new ReadOnlySequence<byte>("persisted-will"u8.ToArray()),
                    CleanStart = false,
                    KeepAliveSeconds = 30,
                    SessionExpiryInterval = 60,
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/persisted"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("value"u8.ToArray())
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            client.Client.LingerState = new LingerOption(true, 0);
            client.Client.Shutdown(SocketShutdown.Both);
            client.Close();

            for (int attempt = 0; attempt < 40; attempt++)
            {
                MqttZeroPersistedSession? saved = sessionStore.Saved;
                if (saved?.Will != null && saved.DisconnectedAt.HasValue)
                {
                    Assert.Equal("persisted-will", saved.ClientId);
                    await app.StopAsync(TestContext.Current.CancellationToken);
                    return;
                }

                await Task.Delay(
                    TimeSpan.FromMilliseconds(50),
                    TestContext.Current.CancellationToken);
            }

            Assert.Fail("The disconnected session with Will was not persisted.");
        }

        [Fact(Timeout = 30_000)]
        public async Task RejectsConnectionWhenSessionPersistenceFails()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroSessionStore, FailingSessionStore>();
            builder.Services.AddMqttZero();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);

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
                    ClientId = new ReadOnlySequence<byte>("failed-save"u8.ToArray()),
                    CleanStart = true
                },
                TestContext.Current.CancellationToken);
            using CancellationTokenSource timeout =
                CancellationTokenSource.CreateLinkedTokenSource(
                    TestContext.Current.CancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] buffer = new byte[1];
            int read = await stream.ReadAsync(buffer, timeout.Token);

            Assert.Equal(0, read);
            Assert.DoesNotContain(
                app.Services.GetRequiredService<MqttZeroBroker>().GetSessions(),
                static session => session.ClientId == "failed-save");
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task CleanStartSaveFailureRestoresExistingSession()
        {
            int port = GetFreePort();
            ControllableSessionStore sessionStore = new();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            builder.Services.AddMqttZero();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(
                    CreatePersistentClientOptions(port, "clean-rollback"),
                    TestContext.Current.CancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter("clean-rollback/#")
                        .Build(),
                    TestContext.Current.CancellationToken);
                await initial.DisconnectAsync(
                    cancellationToken: TestContext.Current.CancellationToken);
            }

            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            await WaitForSessionDisconnectedAsync(
                broker,
                "clean-rollback",
                TestContext.Current.CancellationToken);
            await WaitUntilAsync(
                () => broker.ActiveConnectionCount == 0,
                TestContext.Current.CancellationToken);
            sessionStore.FailNextSave = true;

            using TcpClient rejected = new();
            await rejected.ConnectAsync(
                IPAddress.Loopback,
                port,
                TestContext.Current.CancellationToken);
            NetworkStream stream = rejected.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("clean-rollback"u8.ToArray()),
                    CleanStart = true,
                    SessionExpiryInterval = 60
                },
                TestContext.Current.CancellationToken);
            using CancellationTokenSource closeTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(
                    TestContext.Current.CancellationToken);
            closeTimeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] buffer = new byte[1];
            Assert.Equal(0, await stream.ReadAsync(buffer, closeTimeout.Token));

            MqttZeroSessionSnapshot restored = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "clean-rollback");
            Assert.Equal(1, restored.SubscriptionCount);
            Assert.False(restored.CleanStart);
            Assert.True(sessionStore.Sessions.ContainsKey("clean-rollback"));
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task ReturnsAssignedClientIdentifierForEmptyClientId()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = ReadOnlySequence<byte>.Empty,
                    CleanStart = true
                },
                cancellationToken);

            MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, cancellationToken));
            Assert.False(string.IsNullOrWhiteSpace(connAck.AssignedClientIdentifier));
            await app.StopAsync(cancellationToken);
        }

        private sealed class RecordingSessionStore : IMqttZeroSessionStore
        {
            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public MqttZeroPersistedSession? Saved { get; private set; }

            public ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedSession>>([]);
            }

            public ValueTask SaveAsync(
                MqttZeroPersistedSession session,
                CancellationToken cancellationToken)
            {
                Saved = session;
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteAsync(
                string clientId,
                CancellationToken cancellationToken)
            {
                Saved = null;
                return ValueTask.CompletedTask;
            }
        }

        private sealed class FailingSessionStore : IMqttZeroSessionStore
        {
            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedSession>>([]);
            }

            public ValueTask SaveAsync(
                MqttZeroPersistedSession session,
                CancellationToken cancellationToken)
            {
                throw new InvalidOperationException("Persistence failed.");
            }

            public ValueTask DeleteAsync(
                string clientId,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class ControllableSessionStore : IMqttZeroSessionStore
        {
            private int _failNextSave;

            public bool FailNextSave
            {
                get
                {
                    return Volatile.Read(ref _failNextSave) != 0;
                }
                set
                {
                    Volatile.Write(ref _failNextSave, value ? 1 : 0);
                }
            }

            public Dictionary<string, MqttZeroPersistedSession> Sessions { get; } = [];

            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedSession>>(
                    Sessions.Values.ToArray());
            }

            public ValueTask SaveAsync(
                MqttZeroPersistedSession session,
                CancellationToken cancellationToken)
            {
                if (Interlocked.Exchange(ref _failNextSave, 0) != 0)
                {
                    throw new InvalidOperationException("Save failed.");
                }

                Sessions[session.ClientId] = session;
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteAsync(
                string clientId,
                CancellationToken cancellationToken)
            {
                Sessions.Remove(clientId);
                return ValueTask.CompletedTask;
            }
        }
    }
}
