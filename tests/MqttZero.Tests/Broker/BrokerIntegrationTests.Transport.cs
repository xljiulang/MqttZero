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
        public async Task AppliesAcceptedSocketOptionsBeforeCustomSocketCallback()
        {
            int port = GetFreePort();
            TaskCompletionSource<Socket> configuredSocket = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(
                    IPAddress.Loopback,
                    port,
                    listen => listen.UseMqttZero(options =>
                    {
                        options.AcceptedSockets.NoDelay = false;
                        options.AcceptedSockets.LingerState = new LingerOption(true, 1);
                        options.AcceptedSockets.Configure = (socket, _) =>
                        {
                            configuredSocket.TrySetResult(socket);
                        };
                    }));
            });
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            await client.ConnectAsync(
                CreateClientOptions(port, "socket-options"),
                cancellationToken);

            Socket socket = await configuredSocket.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            Assert.False(socket.NoDelay);
            LingerOption? linger = socket.LingerState;
            Assert.NotNull(linger);
            Assert.True(linger.Enabled);
            Assert.Equal(1, linger.LingerTime);

            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task RoutesMessagesUsingStaticMqttZeroTlsOptions()
        {
            int port = GetFreePort();
            using X509Certificate2 certificate = CreateServerCertificate();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(
                    IPAddress.Loopback,
                    port,
                    listen => listen.UseMqttZero(options =>
                    {
                        options.Tls = new MqttZeroTlsOptions
                        {
                            ServerCertificate = certificate,
                            SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
                        };
                    }));
            });
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await subscriber.ConnectAsync(
                CreateTlsClientOptions(port, "tls-options-subscriber"),
                cancellationToken);
            await publisher.ConnectAsync(
                CreateTlsClientOptions(port, "tls-options-publisher"),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("tls/options")
                    .Build(),
                cancellationToken);
            await publisher.PublishStringAsync(
                "tls/options",
                "configured",
                cancellationToken: cancellationToken);

            Assert.Equal(
                "configured",
                await received.Task.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact]
        public async Task TlsConfigurationRequiresACertificate()
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(
                    IPAddress.Loopback,
                    GetFreePort(),
                    listen => listen.UseMqttZero(options =>
                    {
                        options.Tls = new MqttZeroTlsOptions();
                    }));
            });
            builder.Services.AddMqttZero();

            Assert.Throws<InvalidOperationException>(
                () =>
                {
                    using WebApplication app = builder.Build();
                });
        }

        [Fact(Timeout = 30_000)]
        public async Task AppliesWebSocketFragmentationSelectorFromEndpointMetadata()
        {
            int port = GetFreePort();
            TaskCompletionSource<MqttZeroTransportInfo> selected = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            app.UseWebSockets();
            app.MapMqttZero("/mqtt", options =>
            {
                options.AllowPacketFragmentationSelector = transport =>
                {
                    selected.TrySetResult(transport);
                    return false;
                };
            });
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            await client.ConnectAsync(
                CreateWebSocketClientOptions(port, "ws-fragmentation"),
                cancellationToken);

            MqttZeroTransportInfo transport = await selected.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            Assert.True(transport.IsWebSocket);
            Assert.False(transport.IsSecure);

            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task RoutesPublishedMessageToSubscriber()
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
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            Assert.Equal(MqttZeroBrokerState.Running, broker.State);

            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);

            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            MQTTnet.MqttClientOptions subscriberOptions = CreateClientOptions(port, "subscriber");
            MQTTnet.MqttClientOptions publisherOptions = CreateClientOptions(port, "publisher");

            await subscriber.ConnectAsync(subscriberOptions, cancellationToken);
            await publisher.ConnectAsync(publisherOptions, cancellationToken);
            Assert.True(broker.ConnectedClientCount >= 2);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "devices/+/temperature",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            await publisher.PublishStringAsync(
                "devices/a/temperature",
                "21.5",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            string payload = await received.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            Assert.Equal("21.5", payload);

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task RoutesWebSocketMessagesUsingMqttSubProtocol()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            app.UseWebSockets();
            app.MapMqttZero("/mqtt");

            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);

            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            MQTTnet.MqttClientOptions subscriberOptions = CreateWebSocketClientOptions(port, "ws-subscriber");
            MQTTnet.MqttClientOptions publisherOptions = CreateWebSocketClientOptions(port, "ws-publisher");

            await subscriber.ConnectAsync(subscriberOptions, cancellationToken);
            await publisher.ConnectAsync(publisherOptions, cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "sensors/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            await publisher.PublishStringAsync(
                "sensors/a",
                "ok",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            Assert.Equal("ok", await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 30_000)]
        public async Task RoutesMessagesOverKestrelTls()
        {
            int port = GetFreePort();
            using X509Certificate2 certificate = CreateServerCertificate();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(
                    IPAddress.Loopback,
                    port,
                    listen => listen.UseHttps(certificate).UseMqttZero());
            });
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await subscriber.ConnectAsync(CreateTlsClientOptions(port, "tls-subscriber"), cancellationToken);
            await publisher.ConnectAsync(CreateTlsClientOptions(port, "tls-publisher"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "secure/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            await publisher.PublishStringAsync(
                "secure/data",
                "encrypted",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            Assert.Equal(
                "encrypted",
                await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task DisconnectsIncompletePacketAfterConfiguredTimeout()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options =>
                {
                    options.IncompletePacketTimeout = TimeSpan.FromMilliseconds(200);
                    options.KeepAliveCheckInterval = TimeSpan.FromMilliseconds(50);
                });
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("slow-packet"u8.ToArray()),
                    CleanStart = true
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, cancellationToken));

            await stream.WriteAsync(
                new byte[] { 0x30, 0x7F, 0x00, 0x01, (byte)'a' },
                cancellationToken);
            using CancellationTokenSource closeTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            closeTimeout.CancelAfter(TimeSpan.FromSeconds(3));
            byte[] buffer = new byte[1];
            int read = await stream.ReadAsync(buffer, closeTimeout.Token);

            Assert.Equal(0, read);
            await app.StopAsync(cancellationToken);
        }
    }
}
