using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MQTTnet.Protocol;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Storage;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        private const int RouteGateShardCount = 4;

        [Fact(Timeout = 30_000)]
        public async Task BlockedSubscriptionPersistenceDoesNotBlockOtherSessions()
        {
            int port = GetFreePort();
            BlockingSessionStore store = new();
            await using WebApplication app = await StartStoredBrokerAsync(
                port,
                store,
                options => options.EventLoopShardCount =
                    RouteGateShardCount);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            (string slowClientId, string fastClientId) =
                FindDifferentShardClientIds(RouteGateShardCount);
            MqttClientFactory factory = new();
            using IMqttClient slowClient = factory.CreateMqttClient();
            using IMqttClient fastClient = factory.CreateMqttClient();
            await slowClient.ConnectAsync(
                CreatePersistentClientOptions(port, slowClientId),
                cancellationToken);
            await fastClient.ConnectAsync(
                CreatePersistentClientOptions(port, fastClientId),
                cancellationToken);

            store.Arm();
            Task<MqttClientSubscribeResult> slowSubscribe =
                slowClient.SubscribeAsync(
                    CreateSubscribeOptions("slow/#"),
                    cancellationToken);
            await store.SaveStarted.WaitAsync(
                TimeSpan.FromSeconds(10),
                cancellationToken);

            Task<MqttClientSubscribeResult> fastSubscribe =
                fastClient.SubscribeAsync(
                    CreateSubscribeOptions("fast/#"),
                    cancellationToken);
            MqttClientSubscribeResult fastResult = await fastSubscribe
                .WaitAsync(
                    TimeSpan.FromSeconds(10),
                    cancellationToken);

            Assert.NotEmpty(fastResult.Items);
            Assert.False(slowSubscribe.IsCompleted);

            store.Release();
            MqttClientSubscribeResult slowResult = await slowSubscribe
                .WaitAsync(
                    TimeSpan.FromSeconds(10),
                    cancellationToken);
            Assert.NotEmpty(slowResult.Items);
            Assert.Equal(
                2,
                broker.GetSessionSnapshots().Sum(
                    static session => session.SubscriptionCount));
        }

        [Fact(Timeout = 30_000)]
        public async Task SharedGroupCursorSurvivesWhileAnotherSessionRemains()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient worker1 = factory.CreateMqttClient();
            using IMqttClient worker2 = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            await worker1.ConnectAsync(
                CreatePersistentClientOptions(port, "shared-worker-1"),
                cancellationToken);
            await worker2.ConnectAsync(
                CreatePersistentClientOptions(port, "shared-worker-2"),
                cancellationToken);
            await publisher.ConnectAsync(
                CreatePersistentClientOptions(port, "shared-publisher"),
                cancellationToken);
            const string SharedFilter = "$share/group/shared/topic";
            await worker1.SubscribeAsync(
                CreateSubscribeOptions(SharedFilter),
                cancellationToken);
            await worker2.SubscribeAsync(
                CreateSubscribeOptions(SharedFilter),
                cancellationToken);

            await publisher.PublishAsync(
                CreatePublishOptions("shared/topic"),
                cancellationToken);
            await WaitUntilAsync(
                () => broker.GetMemorySnapshotForTest().SharedCursorCount == 1,
                cancellationToken);

            await worker2.UnsubscribeAsync(
                new MqttClientUnsubscribeOptionsBuilder()
                    .WithTopicFilter(SharedFilter)
                    .Build(),
                cancellationToken);
            Assert.Equal(
                1,
                broker.GetMemorySnapshotForTest().SharedCursorCount);

            await worker1.UnsubscribeAsync(
                new MqttClientUnsubscribeOptionsBuilder()
                    .WithTopicFilter(SharedFilter)
                    .Build(),
                cancellationToken);
            Assert.Equal(
                0,
                broker.GetMemorySnapshotForTest().SharedCursorCount);
        }

        private static MqttClientSubscribeOptions CreateSubscribeOptions(
            string topicFilter)
        {
            return new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(
                    topicFilter,
                    MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();
        }

        private static MqttApplicationMessage CreatePublishOptions(
            string topic)
        {
            return new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(new byte[16])
                .WithQualityOfServiceLevel(
                    MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();
        }

        private static (string First, string Second)
            FindDifferentShardClientIds(int shardCount)
        {
            MqttZeroSessionShardMap map = new(shardCount);
            string first = "route-gate-a";
            int firstShard = map.GetShardId(first);
            for (int index = 0; index < 512; index++)
            {
                string candidate = $"route-gate-b{index}";
                if (map.GetShardId(candidate) != firstShard)
                {
                    return (first, candidate);
                }
            }

            throw new InvalidOperationException(
                "Could not find two client identifiers on different shards.");
        }

        private static async Task WaitUntilAsync(
            Func<bool> predicate,
            CancellationToken cancellationToken)
        {
            using CancellationTokenSource timeout =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            while (!predicate())
            {
                await Task.Delay(
                    TimeSpan.FromMilliseconds(10),
                    timeout.Token);
            }
        }

        private sealed class BlockingSessionStore : IMqttZeroSessionStore
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

            public ValueTask<IReadOnlyList<MqttZeroPersistedSession>>
                LoadAllAsync(CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<
                    IReadOnlyList<MqttZeroPersistedSession>>([]);
            }

            public async ValueTask SaveAsync(
                MqttZeroPersistedSession session,
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
                string clientId,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
