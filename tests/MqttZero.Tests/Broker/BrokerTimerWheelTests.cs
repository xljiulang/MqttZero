using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using MqttZero.AspNetCore;
using MqttZero.Protocol;
using MqttZero.Server.Broker;
using System.Buffers;
using System.Net;
using System.Net.Sockets;
using MqttProtocolVersion = MQTTnet.Formatter.MqttProtocolVersion;
using MqttQualityOfServiceLevel = MqttZero.Protocol.MqttQualityOfServiceLevel;

namespace MqttZero.Tests.Broker;

public sealed partial class BrokerIntegrationTests
{
    [Fact(Timeout = 30_000)]
    public async Task FakeTimeDrivenWheelPublishesKeepAliveWill()
    {
        int port = GetFreePort();
        DateTimeOffset now = new(
            2026,
            11,
            26,
            8,
            0,
            0,
            TimeSpan.Zero);
        MqttZeroTimerFakeTimeProvider timeProvider = new(now);
        await using WebApplication app =
            await StartTimerBrokerAsync(port, timeProvider);
        MqttZeroBroker broker =
            app.Services.GetRequiredService<MqttZeroBroker>();
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttClientFactory factory = new();
        using IMqttClient watcher = factory.CreateMqttClient();
        TaskCompletionSource<string> received = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        watcher.ApplicationMessageReceivedAsync += eventArgs =>
        {
            received.TrySetResult(
                eventArgs.ApplicationMessage.ConvertPayloadToString());
            return Task.CompletedTask;
        };
        await watcher.ConnectAsync(
            CreateClientOptions(port, "timer-watcher"),
            cancellationToken);
        await watcher.SubscribeAsync(
            new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter("timer/will")
                .Build(),
            cancellationToken);

        using System.Net.Sockets.TcpClient idleClient = new();
        await idleClient.ConnectAsync(
            IPAddress.Loopback,
            port,
            cancellationToken);
        NetworkStream stream = idleClient.GetStream();
        ArrayBufferWriter<byte> writer = new();
        MqttPacketCodec.Encode(
            new MqttConnectPacket
            {
                ClientId = new ReadOnlySequence<byte>(
                    "timer-idle"u8.ToArray()),
                CleanStart = true,
                KeepAliveSeconds = 1,
                HasWill = true,
                WillTopic = new ReadOnlySequence<byte>(
                    "timer/will"u8.ToArray()),
                WillPayload = new ReadOnlySequence<byte>(
                    "expired"u8.ToArray()),
                WillQualityOfServiceLevel =
                    MqttQualityOfServiceLevel.AtLeastOnce
            },
            writer);
        await stream.WriteAsync(
            writer.WrittenMemory,
            cancellationToken);
        byte[] connAck = new byte[5];
        _ = await stream.ReadAsync(connAck, cancellationToken);

        timeProvider.Advance(TimeSpan.FromSeconds(2));
        await TickAllShardsAsync(
            broker,
            timeProvider.GetUtcNow(),
            cancellationToken);

        await WaitUntilAsync(
            () => broker.PublishedWillCount == 1,
            cancellationToken);
        Assert.Equal(
            "expired",
            await received.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken));
    }

    [Fact(Timeout = 30_000)]
    public async Task FakeTimeDrivenWheelExpiresDetachedSession()
    {
        int port = GetFreePort();
        DateTimeOffset now = new(
            2026,
            11,
            26,
            9,
            0,
            0,
            TimeSpan.Zero);
        MqttZeroTimerFakeTimeProvider timeProvider = new(now);
        await using WebApplication app =
            await StartTimerBrokerAsync(port, timeProvider);
        MqttZeroBroker broker =
            app.Services.GetRequiredService<MqttZeroBroker>();
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttClientFactory factory = new();
        using IMqttClient client = factory.CreateMqttClient();
        await client.ConnectAsync(
            new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MqttProtocolVersion.V500)
                .WithClientId("timer-expiring-session")
                .WithCleanStart(false)
                .WithSessionExpiryInterval(1)
                .Build(),
            cancellationToken);
        await client.DisconnectAsync(
            cancellationToken: cancellationToken);
        await WaitUntilAsync(
            () => broker.GetSessionSnapshots().Any(
                static session =>
                    session.ClientId == "timer-expiring-session" &&
                    !session.IsConnected),
            cancellationToken);

        timeProvider.Advance(TimeSpan.FromSeconds(2));
        await TickAllShardsAsync(
            broker,
            timeProvider.GetUtcNow(),
            cancellationToken);

        await WaitUntilAsync(
            () => !broker.GetSessionSnapshots().Any(
                static session =>
                    session.ClientId == "timer-expiring-session"),
            cancellationToken);
    }

    [Fact(Timeout = 30_000)]
    public async Task FakeTimeDrivenWheelExpiresQueuedMessageHead()
    {
        int port = GetFreePort();
        DateTimeOffset now = new(
            2026,
            11,
            26,
            10,
            0,
            0,
            TimeSpan.Zero);
        MqttZeroTimerFakeTimeProvider timeProvider = new(now);
        await using WebApplication app =
            await StartTimerBrokerAsync(port, timeProvider);
        MqttZeroBroker broker =
            app.Services.GetRequiredService<MqttZeroBroker>();
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttClientFactory factory = new();
        using IMqttClient subscriber = factory.CreateMqttClient();
        await subscriber.ConnectAsync(
            new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MqttProtocolVersion.V500)
                .WithClientId("timer-offline-subscriber")
                .WithCleanStart(false)
                .WithSessionExpiryInterval(30)
                .Build(),
            cancellationToken);
        await subscriber.SubscribeAsync(
            new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(
                    "timer/queued",
                    MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .Build(),
            cancellationToken);
        await subscriber.DisconnectAsync(
            cancellationToken: cancellationToken);

        using IMqttClient publisher = factory.CreateMqttClient();
        await publisher.ConnectAsync(
            CreateClientOptions(port, "timer-queue-publisher"),
            cancellationToken);
        await publisher.PublishAsync(
            new MqttApplicationMessageBuilder()
                .WithTopic("timer/queued")
                .WithPayload("temporary")
                .WithQualityOfServiceLevel(
                    MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithMessageExpiryInterval(1)
                .Build(),
            cancellationToken);
        await WaitUntilAsync(
            () => broker.GetSessionSnapshots().Any(
                static session =>
                    session.ClientId == "timer-offline-subscriber" &&
                    session.PendingMessageCount == 1),
            cancellationToken);

        timeProvider.Advance(TimeSpan.FromSeconds(2));
        await TickAllShardsAsync(
            broker,
            timeProvider.GetUtcNow(),
            cancellationToken);

        await WaitUntilAsync(
            () => broker.GetSessionSnapshots().Any(
                static session =>
                    session.ClientId == "timer-offline-subscriber" &&
                    session.PendingMessageCount == 0),
            cancellationToken);
    }

    [Fact(Timeout = 30_000)]
    public async Task FakeTimeDrivenWheelExpiresRetainedMessage()
    {
        int port = GetFreePort();
        DateTimeOffset now = new(
            2026,
            11,
            26,
            11,
            0,
            0,
            TimeSpan.Zero);
        MqttZeroTimerFakeTimeProvider timeProvider = new(now);
        await using WebApplication app =
            await StartTimerBrokerAsync(port, timeProvider);
        MqttZeroBroker broker =
            app.Services.GetRequiredService<MqttZeroBroker>();
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttClientFactory factory = new();
        using IMqttClient publisher = factory.CreateMqttClient();
        await publisher.ConnectAsync(
            CreateClientOptions(port, "timer-retained-publisher"),
            cancellationToken);
        await publisher.PublishAsync(
            new MqttApplicationMessageBuilder()
                .WithTopic("timer/retained")
                .WithPayload("temporary")
                .WithRetainFlag()
                .WithQualityOfServiceLevel(
                    MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                .WithMessageExpiryInterval(1)
                .Build(),
            cancellationToken);
        await WaitUntilAsync(
            () => broker.GetRetainedMessageSnapshots().Count == 1,
            cancellationToken);

        timeProvider.Advance(TimeSpan.FromSeconds(2));
        await TickAllShardsAsync(
            broker,
            timeProvider.GetUtcNow(),
            cancellationToken);

        await WaitUntilAsync(
            () => broker.GetRetainedMessageSnapshots().Count == 0,
            cancellationToken);
    }

    [Fact(Timeout = 30_000)]
    public async Task BrokerStopClearsEveryShardTimerWheel()
    {
        int port = GetFreePort();
        DateTimeOffset now = new(
            2026,
            11,
            26,
            12,
            0,
            0,
            TimeSpan.Zero);
        MqttZeroTimerFakeTimeProvider timeProvider = new(now);
        await using WebApplication app =
            await StartTimerBrokerAsync(port, timeProvider);
        MqttZeroBroker broker =
            app.Services.GetRequiredService<MqttZeroBroker>();
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttClientFactory factory = new();
        using IMqttClient client = factory.CreateMqttClient();
        await client.ConnectAsync(
            CreateClientOptions(port, "timer-stop-client"),
            cancellationToken);
        MqttZero.Server.Scheduling.MqttZeroShardTimerWheel[] wheels =
            new MqttZero.Server.Scheduling.MqttZeroShardTimerWheel[
                broker.ShardCount];
        for (int index = 0; index < wheels.Length; index++)
        {
            wheels[index] =
                broker.Shards!.GetTimerWheel(index);
        }

        await broker.StopAsync(cancellationToken);

        Assert.All(
            wheels,
            static wheel => Assert.Equal(0, wheel.Count));
    }

    private static async Task<WebApplication> StartTimerBrokerAsync(
        int port,
        TimeProvider timeProvider)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(
                IPAddress.Loopback,
                port,
                listen => listen.UseMqttZero());
        });
        builder.Services.AddSingleton(timeProvider);
        builder.Services.AddMqttZero();
        WebApplication app = builder.Build();
        await app.StartAsync(TestContext.Current.CancellationToken);
        return app;
    }

    private static async Task TickAllShardsAsync(
        MqttZeroBroker broker,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        for (int shardId = 0; shardId < broker.ShardCount; shardId++)
        {
            MqttZero.Server.Scheduling.MqttZeroEventLoop shard =
                broker.Shards![shardId];
            MqttZero.Server.Scheduling.MqttZeroShardTimerWheel wheel =
                broker.Shards.GetTimerWheel(shardId);
            await shard.InvokeAsync(
                async token =>
                {
                    await wheel.TickAsync(
                        now.UtcTicks,
                        token).ConfigureAwait(false);
                    return true;
                },
                cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class MqttZeroTimerFakeTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow;

        public MqttZeroTimerFakeTimeProvider(DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
        }

        public override DateTimeOffset GetUtcNow()
        {
            return _utcNow;
        }

        public void Advance(TimeSpan duration)
        {
            _utcNow += duration;
        }
    }
}
