using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Protocol;
using MqttZero.AspNetCore;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Storage;
using System.Net;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 60_000)]
        public async Task RetainedMatchCompletesWhileRetainedStoreWriteIsBlocked()
        {
            int port = GetFreePort();
            BlockingRetainedStore store = new();
            await using WebApplication app =
                await StartRetainedStoreBrokerAsync(
                    port,
                    store,
                    options => options.EventLoopShardCount =
                        RouteGateShardCount);
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            (string writerClientId, string readerClientId) =
                FindDifferentShardClientIds(RouteGateShardCount);
            MqttClientFactory factory = new();
            using IMqttClient writer = factory.CreateMqttClient();
            using IMqttClient reader = factory.CreateMqttClient();
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            await writer.ConnectAsync(
                CreatePersistentClientOptions(port, writerClientId),
                cancellationToken);
            await reader.ConnectAsync(
                CreatePersistentClientOptions(port, readerClientId),
                cancellationToken);

            await writer.PublishAsync(
                CreateRetainedPublishOptions("retained/one"),
                cancellationToken);
            await WaitUntilAsync(
                () => broker.GetRetainedMessages().Count == 1,
                cancellationToken);
            store.Arm();
            try
            {
                Task blockedPublish = writer.PublishAsync(
                    CreateRetainedPublishOptions("retained/two"),
                    cancellationToken);
                await store.SaveStarted.WaitAsync(
                    TimeSpan.FromSeconds(20),
                    cancellationToken);

                TaskCompletionSource<string> received = new(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                reader.ApplicationMessageReceivedAsync += message =>
                {
                    received.TrySetResult(message.ApplicationMessage.Topic);
                    return Task.CompletedTask;
                };
                MqttClientSubscribeResult subscribe = await reader
                    .SubscribeAsync(
                        new MqttClientSubscribeOptionsBuilder()
                            .WithTopicFilter(
                                "retained/#",
                                MqttQualityOfServiceLevel.AtLeastOnce)
                            .Build(),
                        cancellationToken)
                    .WaitAsync(
                        TimeSpan.FromSeconds(20),
                        cancellationToken);

                Assert.NotEmpty(subscribe.Items);
                string topic = await received.Task.WaitAsync(
                    TimeSpan.FromSeconds(20),
                    cancellationToken);
                Assert.Equal("retained/one", topic);

                store.Release();
                await blockedPublish.WaitAsync(
                    TimeSpan.FromSeconds(20),
                    cancellationToken);
            }
            finally
            {
                store.Release();
            }
        }

        [Fact(Timeout = 60_000)]
        public async Task ConcurrentRetainedWritesAndSessionChurnComplete()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            Task retainedWriter = Task.Run(
                async () =>
                {
                    using IMqttClient client = factory.CreateMqttClient();
                    await client.ConnectAsync(
                        CreatePersistentClientOptions(
                            port,
                            "retained-churn-writer"),
                        cancellationToken);
                    for (int index = 0; index < 200; index++)
                    {
                        await client.PublishAsync(
                            CreateRetainedPublishOptions(
                                $"retained/churn/{index}"),
                            cancellationToken);
                    }
                },
                cancellationToken);
            Task sessionChurn = Task.Run(
                async () =>
                {
                    for (int index = 0; index < 40; index++)
                    {
                        using IMqttClient client =
                            factory.CreateMqttClient();
                        await client.ConnectAsync(
                            CreatePersistentClientOptions(
                                port,
                                $"retained-churn-{index}"),
                            cancellationToken);
                        await client.SubscribeAsync(
                            new MqttClientSubscribeOptionsBuilder()
                                .WithTopicFilter(
                                    "retained/#",
                                    MqttQualityOfServiceLevel.AtLeastOnce)
                                .Build(),
                            cancellationToken);
                        await client.DisconnectAsync(
                            cancellationToken: cancellationToken);
                    }
                },
                cancellationToken);

            await Task.WhenAll(retainedWriter, sessionChurn)
                .WaitAsync(
                    TimeSpan.FromSeconds(50),
                    cancellationToken);
        }

        private static MqttApplicationMessage CreateRetainedPublishOptions(
            string topic)
        {
            return new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(new byte[32])
                .WithRetainFlag()
                .WithQualityOfServiceLevel(
                    MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();
        }

        private static async Task<WebApplication>
            StartRetainedStoreBrokerAsync(
                int port,
                IMqttZeroRetainedMessageStore retainedStore,
                Action<MqttZeroOptions>? configure = null)
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(
                    IPAddress.Loopback,
                    port,
                    listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<IMqttZeroRetainedMessageStore>(
                retainedStore);
            builder.Services.AddMqttZero(configure);
            WebApplication app = builder.Build();
            await app.StartAsync(
                TestContext.Current.CancellationToken);
            return app;
        }

        private sealed class BlockingRetainedStore :
            IMqttZeroRetainedMessageStore
        {
            private readonly TaskCompletionSource _saveStarted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _release = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private int _armed;

            public Task SaveStarted
            {
                get
                {
                    return _saveStarted.Task;
                }
            }

            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public void Arm()
            {
                Interlocked.Exchange(ref _armed, 1);
            }

            public void Release()
            {
                _release.TrySetResult();
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedRetainedMessage>>
                LoadAllAsync(CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<
                    IReadOnlyList<MqttZeroPersistedRetainedMessage>>([]);
            }

            public async ValueTask SaveAsync(
                MqttZeroPersistedRetainedMessage message,
                CancellationToken cancellationToken)
            {
                if (Interlocked.Exchange(ref _armed, 0) == 0)
                {
                    return;
                }

                _saveStarted.TrySetResult();
                await _release.Task
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            public ValueTask DeleteAsync(
                string topic,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask ClearAsync(CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
