using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Storage;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task SessionExpiryDeadlineRemovesDetachedSession()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
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
                    .WithClientId("expiring-session")
                    .WithCleanStart(false)
                    .WithSessionExpiryInterval(1)
                    .Build(),
                cancellationToken);
            await client.DisconnectAsync(
                cancellationToken: cancellationToken);
            await WaitUntilAsync(
                () => broker.GetSessionSnapshots().Any(
                    static session =>
                        session.ClientId == "expiring-session" &&
                        !session.IsConnected),
                cancellationToken);
            Assert.True(
                broker.GetMemorySnapshotForTest().PendingDeadlineCount > 0);

            await WaitUntilAsync(
                () => !broker.GetSessionSnapshots().Any(
                    static session =>
                        session.ClientId == "expiring-session"),
                cancellationToken);

            Assert.Equal(
                0,
                broker.GetMemorySnapshotForTest().PendingDeadlineCount);
        }

        [Fact(Timeout = 30_000)]
        public async Task RetainedExpiryRetriesAfterDeleteFailure()
        {
            int port = GetFreePort();
            FailingDeleteRetainedStore store = new();
            await using WebApplication app =
                await StartRetainedStoreBrokerAsync(port, store);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(
                CreateClientOptions(port, "retained-retry-publisher"),
                cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("retained/retry")
                    .WithPayload("temporary")
                    .WithRetainFlag()
                    .WithQualityOfServiceLevel(
                        MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithMessageExpiryInterval(1)
                    .Build(),
                cancellationToken);
            await WaitUntilAsync(
                () => store.SaveAttempts == 1,
                cancellationToken);
            await WaitUntilAsync(
                () => store.DeleteAttempts >= 2,
                cancellationToken);

            Assert.Empty(broker.GetRetainedMessageSnapshots());
        }

        private sealed class FailingDeleteRetainedStore :
            IMqttZeroRetainedMessageStore
        {
            private int _deleteAttempts;
            private int _saveAttempts;

            public int DeleteAttempts
            {
                get
                {
                    return Volatile.Read(ref _deleteAttempts);
                }
            }

            public int SaveAttempts
            {
                get
                {
                    return Volatile.Read(ref _saveAttempts);
                }
            }

            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedRetainedMessage>>
                LoadAllAsync(CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<
                    IReadOnlyList<MqttZeroPersistedRetainedMessage>>([]);
            }

            public ValueTask SaveAsync(
                MqttZeroPersistedRetainedMessage message,
                CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _saveAttempts);
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteAsync(
                string topic,
                CancellationToken cancellationToken)
            {
                if (Interlocked.Increment(ref _deleteAttempts) == 1)
                {
                    throw new InvalidOperationException(
                        "The retained delete failed.");
                }

                return ValueTask.CompletedTask;
            }

            public ValueTask ClearAsync(CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
