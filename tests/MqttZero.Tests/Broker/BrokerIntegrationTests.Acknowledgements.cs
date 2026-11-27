using System.Buffers;
using System.Net;
using System.Net.Sockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MQTTnet.Protocol;
using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Sessions;
using MqttZeroQualityOfServiceLevel =
    MqttZero.Protocol.MqttQualityOfServiceLevel;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task PubAckForQos2DeliveryClosesConnectionAndKeepsInflight()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            using TcpClient subscriber = new();
            await subscriber.ConnectAsync(
                IPAddress.Loopback,
                port,
                cancellationToken);
            NetworkStream stream = subscriber.GetStream();
            await ConnectRawSubscriberAsync(
                stream,
                "ack-mismatch-qos2",
                "ack/qos2",
                MqttZeroQualityOfServiceLevel.ExactlyOnce,
                cancellationToken);

            MqttPublishPacket publish = await PublishAndReceiveAsync(
                port,
                "ack/qos2",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce,
                stream,
                cancellationToken);

            await SendPacketAsync(
                stream,
                new MqttPubAckPacket
                {
                    PacketIdentifier = publish.PacketIdentifier,
                    ReasonCode = MqttReasonCode.Success
                },
                cancellationToken);

            await WaitForServerCloseAsync(stream, cancellationToken);
            await AssertInflightSurvivedAsync(
                broker,
                "ack-mismatch-qos2",
                cancellationToken);
        }

        [Fact(Timeout = 30_000)]
        public async Task PubCompForQos1DeliveryClosesConnectionAndKeepsInflight()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            using TcpClient subscriber = new();
            await subscriber.ConnectAsync(
                IPAddress.Loopback,
                port,
                cancellationToken);
            NetworkStream stream = subscriber.GetStream();
            await ConnectRawSubscriberAsync(
                stream,
                "ack-mismatch-qos1",
                "ack/qos1",
                MqttZeroQualityOfServiceLevel.AtLeastOnce,
                cancellationToken);

            MqttPublishPacket publish = await PublishAndReceiveAsync(
                port,
                "ack/qos1",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                stream,
                cancellationToken);

            await SendPacketAsync(
                stream,
                new MqttPubCompPacket
                {
                    PacketIdentifier = publish.PacketIdentifier,
                    ReasonCode = MqttReasonCode.Success
                },
                cancellationToken);

            await WaitForServerCloseAsync(stream, cancellationToken);
            await AssertInflightSurvivedAsync(
                broker,
                "ack-mismatch-qos1",
                cancellationToken);
        }

        private static async Task ConnectRawSubscriberAsync(
            NetworkStream stream,
            string clientId,
            string topicFilter,
            MqttZero.Protocol.MqttQualityOfServiceLevel qualityOfServiceLevel,
            CancellationToken cancellationToken)
        {
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>(
                        System.Text.Encoding.UTF8.GetBytes(clientId)),
                    CleanStart = false,
                    SessionExpiryInterval = 60,
                    KeepAliveSeconds = 0,
                    ProtocolVersion = MqttProtocolVersion.V500
                },
                cancellationToken);
            MqttPacket connAck = await ReadPacketAsync(
                stream,
                cancellationToken);
            Assert.IsType<MqttConnAckPacket>(connAck);
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
                                System.Text.Encoding.UTF8.GetBytes(topicFilter)),
                            QualityOfServiceLevel = qualityOfServiceLevel
                        }
                    ]
                },
                cancellationToken);
            MqttPacket subAck = await ReadPacketAsync(
                stream,
                cancellationToken);
            Assert.IsType<MqttSubAckPacket>(subAck);
        }

        private static async Task<MqttPublishPacket> PublishAndReceiveAsync(
            int port,
            string topic,
            MQTTnet.Protocol.MqttQualityOfServiceLevel qualityOfServiceLevel,
            NetworkStream stream,
            CancellationToken cancellationToken)
        {
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(
                CreatePersistentClientOptions(port, "ack-publisher"),
                cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic(topic)
                    .WithPayload(new byte[64])
                    .WithQualityOfServiceLevel(qualityOfServiceLevel)
                    .Build(),
                cancellationToken);
            MqttPacket received = await ReadPacketAsync(
                stream,
                cancellationToken);
            return Assert.IsType<MqttPublishPacket>(received);
        }

        private static async Task WaitForServerCloseAsync(
            NetworkStream stream,
            CancellationToken cancellationToken)
        {
            using CancellationTokenSource timeout =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            try
            {
                while (true)
                {
                    _ = await ReadPacketAsync(stream, timeout.Token);
                }
            }
            catch (EndOfStreamException)
            {
            }
        }

        private static async Task AssertInflightSurvivedAsync(
            MqttZeroBroker broker,
            string clientId,
            CancellationToken cancellationToken)
        {
            using CancellationTokenSource timeout =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            MqttZeroSessionSnapshot? session = null;
            while (session == null || session.IsConnected)
            {
                session = broker.GetSessionSnapshots()
                    .SingleOrDefault(item => item.ClientId == clientId);
                await Task.Delay(
                    TimeSpan.FromMilliseconds(10),
                    timeout.Token);
            }

            Assert.Equal(1, session.InflightMessageCount);
        }
    }
}
