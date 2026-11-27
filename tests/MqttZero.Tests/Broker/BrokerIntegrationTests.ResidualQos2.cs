using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Sessions;
using MQTTnet;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using System.Buffers;
using System.Net;
using System.Net.Sockets;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task MaxAwaitingRelIsIndependentFromOutboundInflight()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options =>
                {
                    options.MaxInflight = 100;
                    options.MaxAwaitingRel = 1;
                });
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
                    ClientId = new ReadOnlySequence<byte>("awaiting-rel"u8.ToArray())
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));

            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicName = new ReadOnlySequence<byte>("awaiting/rel"u8.ToArray()),
                    Payload = new ReadOnlySequence<byte>("value"u8.ToArray()),
                    QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce,
                    PacketIdentifier = 1
                },
                TestContext.Current.CancellationToken);
            MqttPubRecPacket accepted = Assert.IsType<MqttPubRecPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            Assert.Equal(MqttReasonCode.Success, accepted.ReasonCode);

            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicName = new ReadOnlySequence<byte>("awaiting/rel"u8.ToArray()),
                    Payload = new ReadOnlySequence<byte>("value"u8.ToArray()),
                    QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce,
                    PacketIdentifier = 2
                },
                TestContext.Current.CancellationToken);
            MqttPubRecPacket exceeded = Assert.IsType<MqttPubRecPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            Assert.Equal(MqttReasonCode.ReceiveMaximumExceeded, exceeded.ReasonCode);
        }

        [Fact(Timeout = 30_000)]
        public async Task UnknownInboundPubRelReturnsPacketIdentifierNotFound()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
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
                    ClientId = new ReadOnlySequence<byte>("unknown-pubrel"u8.ToArray()),
                    CleanStart = true
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));

            await SendPacketAsync(
                stream,
                new MqttPubRelPacket
                {
                    PacketIdentifier = 99
                },
                TestContext.Current.CancellationToken);

            MqttPubCompPacket completion = Assert.IsType<MqttPubCompPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            Assert.Equal(
                MqttReasonCode.PacketIdentifierNotFound,
                completion.ReasonCode);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task ErrorPubRecReleasesOutboundQos2Message()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
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
                    ClientId = new ReadOnlySequence<byte>("failed-pubrec"u8.ToArray()),
                    CleanStart = false,
                    SessionExpiryInterval = 60
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
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
                                "failed-pubrec/#"u8.ToArray()),
                            QualityOfServiceLevel =
                                MqttQualityOfServiceLevel.ExactlyOnce
                        }
                    ]
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttSubAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            await broker.PublishAsync(
                "failed-pubrec/value",
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.ExactlyOnce,
                false,
                TestContext.Current.CancellationToken);
            MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));

            await SendPacketAsync(
                stream,
                new MqttPubRecPacket
                {
                    PacketIdentifier = publish.PacketIdentifier,
                    ReasonCode = MqttReasonCode.NotAuthorized
                },
                TestContext.Current.CancellationToken);
            await Task.Delay(
                TimeSpan.FromMilliseconds(250),
                TestContext.Current.CancellationToken);

            MqttZeroSessionSnapshot session = Assert.Single(
                broker.GetSessionSnapshots(),
                current => current.ClientId == "failed-pubrec");
            Assert.Equal(0, session.InflightMessageCount);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task DuplicatePubRecResendsPubRel()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
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
                    ClientId = new ReadOnlySequence<byte>("duplicate-pubrec"u8.ToArray()),
                    CleanStart = false,
                    SessionExpiryInterval = 60
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
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
                                "duplicate-pubrec/#"u8.ToArray()),
                            QualityOfServiceLevel =
                                MqttQualityOfServiceLevel.ExactlyOnce
                        }
                    ]
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttSubAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            await broker.PublishAsync(
                "duplicate-pubrec/value",
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.ExactlyOnce,
                false,
                TestContext.Current.CancellationToken);
            MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            MqttPubRecPacket pubRec = new()
            {
                PacketIdentifier = publish.PacketIdentifier
            };

            await SendPacketAsync(stream, pubRec, TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttPubRelPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            await SendPacketAsync(stream, pubRec, TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttPubRelPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            await SendPacketAsync(
                stream,
                new MqttPubCompPacket
                {
                    PacketIdentifier = publish.PacketIdentifier
                },
                TestContext.Current.CancellationToken);

            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task UnknownOutboundAcknowledgementsAreIgnored()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
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
                    ClientId = new ReadOnlySequence<byte>("unknown-acks"u8.ToArray()),
                    CleanStart = true
                },
                TestContext.Current.CancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));

            await SendPacketAsync(
                stream,
                new MqttPubRecPacket
                {
                    PacketIdentifier = 50
                },
                TestContext.Current.CancellationToken);
            await SendPacketAsync(
                stream,
                new MqttPubCompPacket
                {
                    PacketIdentifier = 51
                },
                TestContext.Current.CancellationToken);
            await Task.Delay(
                TimeSpan.FromMilliseconds(250),
                TestContext.Current.CancellationToken);
            Assert.Equal(1, broker.ConnectedClientCount);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }
    }
}
