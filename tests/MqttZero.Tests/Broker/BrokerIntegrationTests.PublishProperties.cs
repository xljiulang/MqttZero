using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MqttZero.AspNetCore;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Observers;
using MqttZero.Tests.Support;
using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace MqttZero.Tests.Broker;

public sealed partial class BrokerIntegrationTests
{
    [Fact(Timeout = 10_000)]
    public async Task Qos2PublishWaitsForPublishInterceptorBeforePubRec()
    {
        int port = GetFreePort();
        GatePublishInterceptor interceptor = new();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
        builder.Services.AddMqttZero();
        builder.Services.AddSingleton<IMqttZeroPublishInterceptor>(interceptor);
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
                ClientId = new ReadOnlySequence<byte>("qos2-gate"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            stream,
            new MqttPublishPacket
            {
                TopicName = new ReadOnlySequence<byte>("qos2/gate"u8.ToArray()),
                PacketIdentifier = 7,
                QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce
            },
            TestContext.Current.CancellationToken);
        await interceptor.Started.Task.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken);
        using CancellationTokenSource probeTimeout = new(
            TimeSpan.FromMilliseconds(200));
        Task<MqttPacket> pubRecProbe = ReadPacketAsync(
            stream,
            probeTimeout.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await pubRecProbe);
        interceptor.Release();
        _ = Assert.IsType<MqttPubRecPacket>(
            await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = 15_000)]
    public async Task PublishInterceptorCanReplaceTypedProperties()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
        builder.Services.AddMqttZero()
            .AddInterceptor<TaggingPublishInterceptor>();
        await using WebApplication app = builder.Build();
        await app.StartAsync(TestContext.Current.CancellationToken);

        using TcpClient subscriber = new();
        await subscriber.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream stream = subscriber.GetStream();
        await SendPacketAsync(
            stream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("intercepted-properties"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            stream,
            new MqttSubscribePacket
            {
                PacketIdentifier = 1,
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("intercepted/properties"u8.ToArray())
                    }
                ]
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);

        MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
        await broker.PublishAsync(
            new MqttZeroPublishMessage
            {
                Topic = "intercepted/properties",
                Payload = new ReadOnlySequence<byte>("payload"u8.ToArray())
            },
            TestContext.Current.CancellationToken);

        MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(
            await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
        Assert.Equal(
            "interceptor",
            Assert.Single(publish.Properties.UserProperties).Value);
    }

    [Fact(Timeout = 15_000)]
    public async Task DoesNotForwardConnectionLocalTopicAlias()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
        builder.Services.AddMqttZero();
        await using WebApplication app = builder.Build();
        await app.StartAsync(TestContext.Current.CancellationToken);

        using TcpClient subscriber = new();
        await subscriber.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream subscriberStream = subscriber.GetStream();
        await SendPacketAsync(
            subscriberStream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("alias-property-subscriber"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            subscriberStream,
            new MqttSubscribePacket
            {
                PacketIdentifier = 1,
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("alias/property"u8.ToArray())
                    }
                ]
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken);

        using TcpClient publisher = new();
        await publisher.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream publisherStream = publisher.GetStream();
        await SendPacketAsync(
            publisherStream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("alias-property-publisher"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(publisherStream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            publisherStream,
            new MqttPublishPacket
            {
                TopicName = new ReadOnlySequence<byte>("alias/property"u8.ToArray()),
                TopicAlias = 1,
                Payload = new ReadOnlySequence<byte>("payload"u8.ToArray())
            },
            TestContext.Current.CancellationToken);

        MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(
            await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken));
        Assert.Equal("alias/property", Encoding.UTF8.GetString(publish.TopicName.FirstSpan));
        Assert.Null(publish.TopicAlias);
    }

    [Fact(Timeout = 15_000)]
    public async Task RetainedReplayPreservesMessageProperties()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
        builder.Services.AddMqttZero();
        await using WebApplication app = builder.Build();
        await app.StartAsync(TestContext.Current.CancellationToken);

        using TcpClient publisher = new();
        await publisher.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream publisherStream = publisher.GetStream();
        await SendPacketAsync(
            publisherStream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("retained-property-publisher"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(publisherStream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            publisherStream,
            new MqttPublishPacket
            {
                TopicName = new ReadOnlySequence<byte>("retained/properties"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>("retained"u8.ToArray()),
                Retain = true,
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                PacketIdentifier = 1,
                Properties = new MqttPublishProperties
                {
                    ContentType = "text/plain",
                    UserProperties = [new MqttUserProperty("source", "retained")]
                }
            },
            TestContext.Current.CancellationToken);
        _ = Assert.IsType<MqttPubAckPacket>(
            await ReadPacketAsync(publisherStream, TestContext.Current.CancellationToken));

        using TcpClient subscriber = new();
        await subscriber.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream subscriberStream = subscriber.GetStream();
        await SendPacketAsync(
            subscriberStream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("retained-property-subscriber"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            subscriberStream,
            new MqttSubscribePacket
            {
                PacketIdentifier = 1,
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("retained/#"u8.ToArray())
                    }
                ]
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken);

        MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(
            await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken));
        Assert.Equal("text/plain", publish.Properties.ContentType);
        Assert.Equal(
            "retained",
            Assert.Single(publish.Properties.UserProperties).Value);
    }

    [Fact(Timeout = 15_000)]
    public async Task WillPublishPreservesMessageProperties()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
        builder.Services.AddMqttZero();
        await using WebApplication app = builder.Build();
        await app.StartAsync(TestContext.Current.CancellationToken);

        using TcpClient subscriber = new();
        await subscriber.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream subscriberStream = subscriber.GetStream();
        await SendPacketAsync(
            subscriberStream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("will-property-subscriber"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            subscriberStream,
            new MqttSubscribePacket
            {
                PacketIdentifier = 1,
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("will/properties"u8.ToArray())
                    }
                ]
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken);

        using TcpClient willClient = new();
        await willClient.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream willStream = willClient.GetStream();
        await SendPacketAsync(
            willStream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("will-property-client"u8.ToArray()),
                HasWill = true,
                WillTopic = new ReadOnlySequence<byte>("will/properties"u8.ToArray()),
                WillPayload = new ReadOnlySequence<byte>("offline"u8.ToArray()),
                WillProperties = new MqttPublishProperties
                {
                    ContentType = "text/plain",
                    UserProperties = [new MqttUserProperty("source", "will")]
                }
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(willStream, TestContext.Current.CancellationToken);
        willClient.Client.Shutdown(SocketShutdown.Both);
        willClient.Close();

        MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(
            await ReadPacketAsync(subscriberStream, TestContext.Current.CancellationToken));
        Assert.Equal("will/properties", Encoding.UTF8.GetString(publish.TopicName.FirstSpan));
        Assert.Equal("text/plain", publish.Properties.ContentType);
        Assert.Equal("will", Assert.Single(publish.Properties.UserProperties).Value);
    }

    [Fact(Timeout = 15_000)]
    public async Task DropsPublishThatExceedsReceiverMaximumPacketSize()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
        builder.Services.AddSingleton<PacketTooLargeObserver>();
        builder.Services.AddMqttZero()
            .AddDeliveryObserver<PacketTooLargeObserver>();
        await using WebApplication app = builder.Build();
        await app.StartAsync(TestContext.Current.CancellationToken);

        using TcpClient subscriber = new();
        await subscriber.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream stream = subscriber.GetStream();
        await SendPacketAsync(
            stream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("small-packet-subscriber"u8.ToArray()),
                MaximumPacketSize = 64
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            stream,
            new MqttSubscribePacket
            {
                PacketIdentifier = 1,
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("packet/limit"u8.ToArray())
                    }
                ]
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);

        MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
        PacketTooLargeObserver observer =
            app.Services.GetRequiredService<PacketTooLargeObserver>();
        await broker.PublishAsync(
            new MqttZeroPublishMessage
            {
                Topic = "packet/limit",
                Payload = new ReadOnlySequence<byte>(new byte[128])
            },
            TestContext.Current.CancellationToken);

        MqttZeroMessageDropReason reason = await observer.Reason.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken);
        Assert.Equal(MqttZeroMessageDropReason.PacketTooLarge, reason);
        Assert.True(broker.TryGetClient("small-packet-subscriber", out var connected));
        Assert.True(connected!.IsConnected);
    }

    [Fact(Timeout = 15_000)]
    public async Task ClientPublishSubscriptionIdentifierIsRejected()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
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
                ClientId = new ReadOnlySequence<byte>("invalid-subscription-id"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);

        await SendPacketAsync(
            stream,
            new MqttPublishPacket
            {
                TopicName = new ReadOnlySequence<byte>("properties/value"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>("payload"u8.ToArray()),
                SubscriptionIdentifiers = [7]
            },
            TestContext.Current.CancellationToken);

        MqttDisconnectPacket disconnect = Assert.IsType<MqttDisconnectPacket>(
            await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
        Assert.Equal(MqttReasonCode.ProtocolError, disconnect.ReasonCode);
    }

    [Fact(Timeout = 15_000)]
    public async Task InvalidUtf8PayloadGetsQos1PayloadFormatInvalid()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
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
                ClientId = new ReadOnlySequence<byte>("invalid-utf8-qos1"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);

        await SendPacketAsync(
            stream,
            new MqttPublishPacket
            {
                TopicName = new ReadOnlySequence<byte>("properties/value"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>([0xFF]),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                PacketIdentifier = 10,
                Properties = new MqttPublishProperties
                {
                    PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8
                }
            },
            TestContext.Current.CancellationToken);

        MqttPubAckPacket pubAck = Assert.IsType<MqttPubAckPacket>(
            await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
        Assert.Equal(10, pubAck.PacketIdentifier);
        Assert.Equal(MqttReasonCode.PayloadFormatInvalid, pubAck.ReasonCode);
    }

    [Fact(Timeout = 15_000)]
    public async Task InvalidUtf8PayloadClosesQos0Publisher()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
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
                ClientId = new ReadOnlySequence<byte>("invalid-utf8-qos0"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);

        await SendPacketAsync(
            stream,
            new MqttPublishPacket
            {
                TopicName = new ReadOnlySequence<byte>("properties/value"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>([0xFF]),
                Properties = new MqttPublishProperties
                {
                    PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8
                }
            },
            TestContext.Current.CancellationToken);

        MqttDisconnectPacket disconnect = Assert.IsType<MqttDisconnectPacket>(
            await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
        Assert.Equal(MqttReasonCode.PayloadFormatInvalid, disconnect.ReasonCode);
    }

    [Fact(Timeout = 15_000)]
    public async Task InvalidUtf8PayloadGetsQos2PayloadFormatInvalid()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
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
                ClientId = new ReadOnlySequence<byte>("invalid-utf8-qos2"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);

        byte[] encoded = ProtocolPacketTestHelper.Encode(
            new MqttPublishPacket
            {
                TopicName = new ReadOnlySequence<byte>("properties/value"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>([0xFF]),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce,
                PacketIdentifier = 11,
                Properties = new MqttPublishProperties
                {
                    PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8
                }
            });
        await stream.WriteAsync(
            encoded,
            TestContext.Current.CancellationToken);
        await stream.FlushAsync(TestContext.Current.CancellationToken);

        MqttPubRecPacket pubRec = Assert.IsType<MqttPubRecPacket>(
            await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
        Assert.Equal(11, pubRec.PacketIdentifier);
        Assert.Equal(MqttReasonCode.PayloadFormatInvalid, pubRec.ReasonCode);
    }

    [Fact(Timeout = 15_000)]
    public async Task ServerPublishForwardsCompleteMessageProperties()
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
            kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
        builder.Services.AddMqttZero();
        await using WebApplication app = builder.Build();
        await app.StartAsync(TestContext.Current.CancellationToken);

        using TcpClient subscriber = new();
        await subscriber.ConnectAsync(
            IPAddress.Loopback,
            port,
            TestContext.Current.CancellationToken);
        NetworkStream stream = subscriber.GetStream();
        await SendPacketAsync(
            stream,
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>("property-subscriber"u8.ToArray())
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);
        await SendPacketAsync(
            stream,
            new MqttSubscribePacket
            {
                PacketIdentifier = 1,
                UserProperties = [new MqttUserProperty("tenant", "east")],
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("properties/#"u8.ToArray())
                    }
                ]
            },
            TestContext.Current.CancellationToken);
        _ = await ReadPacketAsync(stream, TestContext.Current.CancellationToken);

        MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
        await broker.PublishAsync(
            new MqttZeroPublishMessage
            {
                Topic = "properties/value",
                Payload = new ReadOnlySequence<byte>("payload"u8.ToArray()),
                Properties = new MqttPublishProperties
                {
                    PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8,
                    ContentType = "text/plain",
                    ResponseTopic = "properties/response",
                    CorrelationData = "correlation"u8.ToArray(),
                    UserProperties =
                    [
                        new MqttUserProperty("trace", "one"),
                        new MqttUserProperty("trace", "two")
                    ]
                }
            },
            TestContext.Current.CancellationToken);

        MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(
            await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
        Assert.Equal(
            MqttPayloadFormatIndicator.Utf8,
            publish.Properties.PayloadFormatIndicator);
        Assert.Equal("text/plain", publish.Properties.ContentType);
        Assert.Equal("properties/response", publish.Properties.ResponseTopic);
        Assert.Equal(
            "correlation",
            Encoding.UTF8.GetString(publish.Properties.CorrelationData!.Value.Span));
        Assert.Collection(
            publish.Properties.UserProperties,
            property => Assert.Equal(("trace", "one"), (property.Name, property.Value)),
            property => Assert.Equal(("trace", "two"), (property.Name, property.Value)));
    }

    private sealed class PacketTooLargeObserver : IMqttZeroDeliveryObserver
    {
        public TaskCompletionSource<MqttZeroMessageDropReason> Reason { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask OnMessageEnqueuedAsync(
            MqttZeroMessageEnqueuedContext context,
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask OnMessageDroppedAsync(
            MqttZeroMessageDroppedContext context,
            CancellationToken cancellationToken)
        {
            if (context.Reason == MqttZeroMessageDropReason.PacketTooLarge)
            {
                Reason.TrySetResult(context.Reason);
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask OnQueueMessageOverwrittenAsync(
            MqttZeroQueueMessageOverwrittenContext context,
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask OnMessageNotConsumedAsync(
            MqttZeroMessageNotConsumedContext context,
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TaggingPublishInterceptor : IMqttZeroPublishInterceptor
    {
        public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
            MqttZeroPublishContext context,
            CancellationToken cancellationToken)
        {
            context.Properties = new MqttPublishProperties
            {
                ContentType = "text/plain",
                UserProperties = [new MqttUserProperty("source", "interceptor")]
            };
            return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
        }
    }

    private sealed class GatePublishInterceptor : IMqttZeroPublishInterceptor
    {
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Started { get; } = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release()
        {
            _release.TrySetResult();
        }

        public async ValueTask<MqttZeroInterceptionResult> InterceptAsync(
            MqttZeroPublishContext context,
            CancellationToken cancellationToken)
        {
            Started.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
            return MqttZeroInterceptionResult.Continue;
        }
    }
}
