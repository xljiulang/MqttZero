using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MQTTnet.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Retained;
using MqttZero.Server.Storage;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task RetainedManagementReadKeepsExpiredEntriesForTheDeadlineTask()
        {
            int port = GetFreePort();
            BlockingDeleteRetainedStore store = new();
            await using WebApplication app =
                await StartRetainedStoreBrokerAsync(port, store);
            MqttZeroBroker broker =
                app.Services.GetRequiredService<MqttZeroBroker>();
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(
                CreateClientOptions(port, "retained-read-publisher"),
                cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("retained/managed")
                    .WithPayload("temporary")
                    .WithRetainFlag()
                    .WithQualityOfServiceLevel(
                        MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithMessageExpiryInterval(1)
                    .Build(),
                cancellationToken);
            await store.DeleteStarted.WaitAsync(
                TimeSpan.FromSeconds(10),
                cancellationToken);

            _ = broker.GetRetainedMessageSnapshots();

            List<MqttZeroRetainedMessage> matches = [];
            broker.RetainedTopicTrieForTest.Match(
                "retained/#"u8,
                matches);
            Assert.Single(matches);

            store.Release();
            await WaitUntilAsync(
                () => broker.GetRetainedMessageSnapshots().Count == 0,
                cancellationToken);
            Assert.Equal(1, store.DeleteAttempts);
        }

        [Fact(Timeout = 30_000)]
        public async Task BrokerStartsDespiteRetainedRecordWithInvalidProperties()
        {
            int port = GetFreePort();
            InvalidPropertiesRetainedStore store = new();
            await using WebApplication app =
                await StartRetainedStoreBrokerAsync(port, store);
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += message =>
            {
                received.TrySetResult(message.ApplicationMessage.Topic);
                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(
                CreateClientOptions(port, "retained-invalid-subscriber"),
                cancellationToken);
            MqttClientSubscribeResult subscribe = await subscriber
                .SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "retained/#",
                            MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken)
                .WaitAsync(
                    TimeSpan.FromSeconds(10),
                    cancellationToken);

            Assert.NotEmpty(subscribe.Items);
            Assert.Equal(
                "retained/good",
                await received.Task.WaitAsync(
                    TimeSpan.FromSeconds(10),
                    cancellationToken));
            Assert.Equal(1, store.DeleteAttempts);
            Assert.True(subscriber.IsConnected);
        }

        private sealed class BlockingDeleteRetainedStore :
            IMqttZeroRetainedMessageStore
        {
            private readonly TaskCompletionSource _deleteStarted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _release = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private int _deleteAttempts;

            public Task DeleteStarted
            {
                get
                {
                    return _deleteStarted.Task;
                }
            }

            public int DeleteAttempts
            {
                get
                {
                    return Volatile.Read(ref _deleteAttempts);
                }
            }

            public bool IsEnabled
            {
                get
                {
                    return true;
                }
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

            public ValueTask SaveAsync(
                MqttZeroPersistedRetainedMessage message,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public async ValueTask DeleteAsync(
                string topic,
                CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _deleteAttempts);
                _deleteStarted.TrySetResult();
                await _release.Task
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }

            public ValueTask ClearAsync(CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class InvalidPropertiesRetainedStore :
            IMqttZeroRetainedMessageStore
        {
            private int _deleteAttempts;

            public int DeleteAttempts
            {
                get
                {
                    return Volatile.Read(ref _deleteAttempts);
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
                MqttZeroPersistedRetainedMessage message = new()
                {
                    Topic = "bad/topic"u8.ToArray(),
                    Properties = new byte[] { 0x02 },
                    Payload = new byte[] { 0x01 },
                    QualityOfServiceLevel =
                        MqttZero.Protocol.MqttQualityOfServiceLevel.AtLeastOnce
                };
                MqttZeroPersistedRetainedMessage good = new()
                {
                    Topic = "retained/good"u8.ToArray(),
                    Payload = new byte[] { 0x02 },
                    QualityOfServiceLevel =
                        MqttZero.Protocol.MqttQualityOfServiceLevel.AtLeastOnce
                };
                return ValueTask.FromResult<
                    IReadOnlyList<MqttZeroPersistedRetainedMessage>>(
                    [message, good]);
            }

            public ValueTask SaveAsync(
                MqttZeroPersistedRetainedMessage message,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteAsync(
                string topic,
                CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _deleteAttempts);
                return ValueTask.CompletedTask;
            }

            public ValueTask ClearAsync(CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
