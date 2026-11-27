using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttZero.AspNetCore;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using MqttZero.Protocol;
using System.Collections.Concurrent;
using System.Net;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task DeferredQosZeroBlocksLaterDirectPublishes()
        {
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(
                    IPAddress.Loopback,
                    port,
                    listen => listen.UseMqttZero()));
            builder.Services.AddMqttZero(
                options => options.EventLoopShardCount = 1);
            await using WebApplication app = builder.Build();
            await app.StartAsync(cancellationToken);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            ConcurrentQueue<string> received = new();
            TaskCompletionSource<bool> bothReceived = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.Enqueue(
                    eventArgs.ApplicationMessage.ConvertPayloadToString());
                if (received.Count >= 2)
                {
                    bothReceived.TrySetResult(true);
                }

                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "ordering-subscriber"),
                cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("ordering/#")
                    .Build(),
                cancellationToken);

            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(
                CreateClientOptions(port, "ordering-publisher"),
                cancellationToken);

            MqttZeroShardSet shards = broker.Shards!;
            TaskCompletionSource blockerStarted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseBlocker = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            await shards[0].EnqueueAsync(
                new BlockingShardWork(blockerStarted, releaseBlocker),
                cancellationToken);
            await blockerStarted.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            try
            {
                await publisher.PublishAsync(
                    new MqttApplicationMessageBuilder()
                        .WithTopic("ordering/topic")
                        .WithPayload("retained")
                        .WithRetainFlag()
                        .Build(),
                    cancellationToken);
                await publisher.PublishAsync(
                    new MqttApplicationMessageBuilder()
                        .WithTopic("ordering/topic")
                        .WithPayload("plain")
                        .Build(),
                    cancellationToken);
            }
            finally
            {
                releaseBlocker.TrySetResult();
            }

            await bothReceived.Task.WaitAsync(
                TimeSpan.FromSeconds(10),
                cancellationToken);

            Assert.Equal(["retained", "plain"], received.ToArray());
            await app.StopAsync(cancellationToken);
        }

        private sealed class BlockingShardWork : MqttZeroShardWork
        {
            private readonly TaskCompletionSource _release;
            private readonly TaskCompletionSource _started;

            public BlockingShardWork(
                TaskCompletionSource started,
                TaskCompletionSource release)
            {
                _started = started;
                _release = release;
            }

            public override async ValueTask ExecuteAsync(
                MqttZeroEventLoop shard,
                CancellationToken cancellationToken)
            {
                _started.TrySetResult();
                await _release.Task.WaitAsync(cancellationToken);
            }
        }
    }
}
