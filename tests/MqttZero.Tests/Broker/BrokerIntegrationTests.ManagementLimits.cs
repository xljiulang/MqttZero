using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
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
        public async Task ManagementApiDisconnectsConnectedClient()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            TaskCompletionSource<MQTTnet.MqttClientDisconnectReason> disconnected =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DisconnectedAsync += eventArgs =>
            {
                disconnected.TrySetResult(eventArgs.Reason);
                return Task.CompletedTask;
            };

            await client.ConnectAsync(CreateClientOptions(port, "managed-client"), cancellationToken);
            bool found = await broker.DisconnectClientAsync(
                "managed-client",
                MqttReasonCode.ServerShuttingDown,
                cancellationToken);

            Assert.True(found);
            Assert.Equal(
                MQTTnet.MqttClientDisconnectReason.ServerShuttingDown,
                await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task ManagementApiExposesTypedKestrelRemoteEndPoint()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            await client.ConnectAsync(
                CreateClientOptions(port, "endpoint-client"),
                cancellationToken);

            MqttZeroClientSnapshot snapshot = Assert.Single(broker.GetClientSnapshots());
            IPEndPoint remoteEndPoint = Assert.IsType<IPEndPoint>(snapshot.RemoteEndPoint);
            Assert.True(IPAddress.IsLoopback(remoteEndPoint.Address));
            Assert.True(remoteEndPoint.Port > 0);

            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task ManagementQueriesReturnFalseForUnknownAndOfflineClients()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();

            using (IMqttClient client = factory.CreateMqttClient())
            {
                await client.ConnectAsync(
                    CreatePersistentClientOptions(port, "offline-management"),
                    cancellationToken);
                await client.DisconnectAsync(cancellationToken: cancellationToken);
            }

            await WaitForSessionDisconnectedAsync(
                broker,
                "offline-management",
                cancellationToken);

            Assert.False(broker.TryGetClient("missing", out MqttZeroConnectedClient? missing));
            Assert.Null(missing);
            Assert.Throws<ArgumentException>(() =>
                broker.TryGetClient(string.Empty, out _));
            Assert.False(broker.TryGetClient(
                "offline-management",
                out MqttZeroConnectedClient? offline));
            Assert.Null(offline);
            Assert.False(await broker.DisconnectClientAsync(
                "missing",
                MqttReasonCode.Success,
                cancellationToken));
            Assert.False(await broker.DisconnectClientAsync(
                "offline-management",
                MqttReasonCode.Success,
                cancellationToken));
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await broker.DisconnectClientAsync(
                    string.Empty,
                    MqttReasonCode.Success,
                    cancellationToken));
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task ClosesConnectionForOversizedInboundPacket()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options => options.MaxPacketSize = 64);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("oversized-client"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 30
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));

            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicName = new ReadOnlySequence<byte>("oversized/topic"u8.ToArray()),
                    Payload = new ReadOnlySequence<byte>(new byte[128])
                },
                cancellationToken);

            using CancellationTokenSource readTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            readTimeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] buffer = new byte[1];
            int read = await stream.ReadAsync(buffer, readTimeout.Token);
            Assert.Equal(0, read);
            await app.StopAsync(cancellationToken);
        }
    }
}
