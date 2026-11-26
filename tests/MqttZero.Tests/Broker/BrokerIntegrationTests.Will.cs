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
        public async Task PublishesWillAfterUnexpectedDisconnect()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient watcher = factory.CreateMqttClient();
            TaskCompletionSource<string> willReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
            watcher.ApplicationMessageReceivedAsync += eventArgs =>
            {
                willReceived.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            await watcher.ConnectAsync(CreateClientOptions(port, "will-watcher"), cancellationToken);
            await watcher.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "will/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            Assert.Equal(
                1,
                Assert.Single(
                    broker.GetSessions(),
                    session => session.ClientId == "will-watcher").SubscriptionCount);

            using TcpClient abortedClient = new();
            await abortedClient.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = abortedClient.GetStream();
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("aborted-client"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 30,
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/aborted"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("gone"u8.ToArray()),
                    WillQualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                },
                writer);
            Assert.True(
                MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(writer.WrittenMemory),
                    out MqttPacket? encodedConnect,
                    out _));
            MqttConnectPacket encodedConnectPacket = Assert.IsType<MqttConnectPacket>(encodedConnect);
            Assert.Equal(
                "will/aborted",
                System.Text.Encoding.UTF8.GetString(encodedConnectPacket.WillTopic.FirstSpan));
            await stream.WriteAsync(writer.WrittenMemory, cancellationToken);
            byte[] connAck = new byte[5];
            _ = await stream.ReadAsync(connAck, cancellationToken);
            MqttZeroSessionSnapshot connectedAbortedSession = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "aborted-client");
            Assert.True(connectedAbortedSession.HasWill);
            Assert.Equal("will/aborted", connectedAbortedSession.WillTopic);
            abortedClient.Client.Shutdown(SocketShutdown.Both);
            abortedClient.Client.LingerState = new LingerOption(true, 0);
            abortedClient.Close();

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            Assert.Equal(1, broker.PublishedWillCount);

            Assert.Equal(
                "gone",
                await willReceived.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await watcher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task DoesNotPublishWillAfterCleanDisconnect()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            using TcpClient client = new();
            await client.ConnectAsync(
                IPAddress.Loopback,
                port,
                cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("clean-will-client"u8.ToArray()),
                    CleanStart = true,
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/clean"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("unexpected"u8.ToArray()),
                    WillQualityOfServiceLevel =
                        MqttQualityOfServiceLevel.AtLeastOnce
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, cancellationToken));
            await SendPacketAsync(
                stream,
                new MqttDisconnectPacket(),
                cancellationToken);
            byte[] buffer = new byte[1];
            _ = await stream.ReadAsync(buffer, cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.Equal(0, broker.PublishedWillCount);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task PublishesWillAfterKeepAliveTimeout()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient watcher = factory.CreateMqttClient();
            TaskCompletionSource<string> willReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
            watcher.ApplicationMessageReceivedAsync += eventArgs =>
            {
                willReceived.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            await watcher.ConnectAsync(CreateClientOptions(port, "keepalive-watcher"), cancellationToken);
            await watcher.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "will/keepalive",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            using TcpClient idleClient = new();
            await idleClient.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = idleClient.GetStream();
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("idle-client"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 1,
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/keepalive"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("timeout"u8.ToArray()),
                    WillQualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                },
                writer);
            await stream.WriteAsync(writer.WrittenMemory, cancellationToken);
            byte[] connAck = new byte[5];
            _ = await stream.ReadAsync(connAck, cancellationToken);

            Assert.Equal(
                "timeout",
                await willReceived.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await watcher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task PublishesWillWhenClientIdIsTakenOver()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient watcher = factory.CreateMqttClient();
            TaskCompletionSource<string> willReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
            watcher.ApplicationMessageReceivedAsync += eventArgs =>
            {
                willReceived.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            await watcher.ConnectAsync(CreateClientOptions(port, "takeover-will-watcher"), cancellationToken);
            await watcher.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "will/takeover",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            using TcpClient first = new();
            await first.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = first.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("takeover-will-client"u8.ToArray()),
                    CleanStart = false,
                    KeepAliveSeconds = 30,
                    SessionExpiryInterval = 60,
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/takeover"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("replaced"u8.ToArray())
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));

            using IMqttClient second = factory.CreateMqttClient();
            _ = await second.ConnectAsync(
                CreatePersistentClientOptions(port, "takeover-will-client"),
                cancellationToken);

            Assert.Equal(
                "replaced",
                await willReceived.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await second.DisconnectAsync(cancellationToken: cancellationToken);
            await watcher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task ReconnectCancelsDelayedWill()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();

            using (TcpClient abortedClient = new())
            {
                await abortedClient.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
                NetworkStream stream = abortedClient.GetStream();
                ArrayBufferWriter<byte> writer = new();
                MqttPacketCodec.Encode(
                    new MqttConnectPacket
                    {
                        ClientId = new ReadOnlySequence<byte>("delayed-will-client"u8.ToArray()),
                        CleanStart = false,
                        KeepAliveSeconds = 30,
                        SessionExpiryInterval = 60,
                        HasWill = true,
                        WillTopic = new ReadOnlySequence<byte>("will/delayed"u8.ToArray()),
                        WillPayload = new ReadOnlySequence<byte>("should-not-publish"u8.ToArray()),
                        WillDelayInterval = 2
                    },
                    writer);
                await stream.WriteAsync(writer.WrittenMemory, cancellationToken);
                byte[] connAck = new byte[5];
                _ = await stream.ReadAsync(connAck, cancellationToken);
                abortedClient.Client.LingerState = new LingerOption(true, 0);
                abortedClient.Client.Shutdown(SocketShutdown.Both);
                abortedClient.Close();
            }

            MqttClientFactory factory = new();
            using IMqttClient restored = factory.CreateMqttClient();
            MQTTnet.MqttClientConnectResult result =
                await restored.ConnectAsync(CreatePersistentClientOptions(port, "delayed-will-client"), cancellationToken);
            Assert.True(result.IsSessionPresent);

            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.Equal(0, broker.PublishedWillCount);

            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
    }
}
