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
        private static MQTTnet.MqttClientOptions CreateClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart()
                .Build();
        }

        private static MQTTnet.MqttClientOptions CreatePersistentClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart(false)
                .WithSessionExpiryInterval(60)
                .Build();
        }

        private static MQTTnet.MqttClientOptions CreateWebSocketClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithWebSocketServer(options =>
                {
                    options.WithUri($"ws://127.0.0.1:{port}/mqtt");
                })
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart()
                .Build();
        }

        private static MQTTnet.MqttClientOptions CreateTlsClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart()
                .WithTlsOptions(options =>
                {
                    options
                        .UseTls()
                        .WithSslProtocols(SslProtocols.Tls12 | SslProtocols.Tls13)
                        .WithAllowUntrustedCertificates()
                        .WithIgnoreCertificateChainErrors()
                        .WithCertificateValidationHandler(static _ => true);
                })
                .Build();
        }

        private static X509Certificate2 CreateServerCertificate()
        {
            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new(
                "CN=localhost",
                rsa,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            SubjectAlternativeNameBuilder alternativeNames = new();
            alternativeNames.AddDnsName("localhost");
            alternativeNames.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(alternativeNames.Build());
            request.CertificateExtensions.Add(
                new X509BasicConstraintsExtension(false, false, 0, false));
            request.CertificateExtensions.Add(
                new X509KeyUsageExtension(
                    X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
                    false));
            request.CertificateExtensions.Add(
                new X509EnhancedKeyUsageExtension(
                    new OidCollection
                    {
                        new("1.3.6.1.5.5.7.3.1")
                    },
                    false));
            using X509Certificate2 temporary = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddMinutes(-1),
                DateTimeOffset.UtcNow.AddDays(1));
            return X509CertificateLoader.LoadPkcs12(
                temporary.Export(X509ContentType.Pkcs12),
                null,
                X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
        }

        private static async Task<WebApplication> StartTcpBrokerAsync(
            int port,
            Action<MqttZeroOptions>? configure = null)
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero());
            });
            builder.Services.AddMqttZero(configure);

            WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            return app;
        }

        private static async Task SendPacketAsync(
            NetworkStream stream,
            MqttPacket packet,
            CancellationToken cancellationToken)
        {
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(packet, writer);
            await stream.WriteAsync(writer.WrittenMemory, cancellationToken);
        }

        private static async Task<MqttPacket> ReadPacketAsync(
            NetworkStream stream,
            CancellationToken cancellationToken)
        {
            byte[] buffer = new byte[65536];
            int count = 0;
            while (count < buffer.Length)
            {
                int read = await stream.ReadAsync(buffer.AsMemory(count, 1), cancellationToken);
                if (read == 0)
                {
                    throw new EndOfStreamException();
                }

                count += read;
                if (MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(buffer, 0, count),
                    out MqttPacket? packet,
                    out _))
                {
                    return packet ?? throw new InvalidOperationException("Decoded packet was null.");
                }
            }

            throw new InvalidOperationException("MQTT packet exceeded the test buffer.");
        }

        private static async Task WaitForSessionDisconnectedAsync(
            MqttZeroBroker broker,
            string clientId,
            CancellationToken cancellationToken)
        {
            for (int attempt = 0; attempt < 200; attempt++)
            {
                MqttZeroSessionSnapshot? session = broker.GetSessionSnapshots()
                    .SingleOrDefault(item => item.ClientId == clientId);
                if (session != null && !session.IsConnected)
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
            }

            throw new TimeoutException($"Session '{clientId}' did not disconnect.");
        }

        private static int GetFreePort()
        {
            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
