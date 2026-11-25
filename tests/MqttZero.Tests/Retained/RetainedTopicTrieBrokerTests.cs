using Microsoft.Extensions.DependencyInjection;
using MqttZero.AspNetCore;
using MqttZero.Protocol;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Retained;
using MqttZero.Server.Storage;
using System.Buffers;

namespace MqttZero.Tests.Retained;

public sealed class RetainedTopicTrieBrokerTests
{
    [Fact]
    public async Task ZeroByteRetainedPublishRemovesTopicFromTrie()
    {
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            new MqttZeroOptions(),
            TimeProvider.System);
        await broker.StartAsync(TestContext.Current.CancellationToken);

        await broker.PublishAsync(
            "devices/a/state",
            new ReadOnlySequence<byte>([1]),
            MqttQualityOfServiceLevel.AtMostOnce,
            true,
            TestContext.Current.CancellationToken);
        Assert.Equal(
            1,
            CountMatches(broker, "devices/+/state"));

        await broker.PublishAsync(
            "devices/a/state",
            ReadOnlySequence<byte>.Empty,
            MqttQualityOfServiceLevel.AtMostOnce,
            true,
            TestContext.Current.CancellationToken);
        Assert.Equal(
            0,
            CountMatches(broker, "devices/+/state"));
    }

    [Fact]
    public async Task FailedRetainedSaveLeavesTrieUnchanged()
    {
        RecordingRetainedStore store = new()
        {
            FailNextSave = true
        };
        ServiceCollection services = new();
        services.AddSingleton<IMqttZeroRetainedMessageStore>(store);
        services.AddMqttZero();
        await using ServiceProvider provider = services.BuildServiceProvider();
        MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();
        await broker.StartAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            broker.PublishAsync(
                "failure/topic",
                new ReadOnlySequence<byte>([1]),
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(0, CountMatches(broker, "failure/#"));
        Assert.Empty(broker.GetRetainedMessages());

        await broker.PublishAsync(
            "failure/topic",
            new ReadOnlySequence<byte>([1]),
            MqttQualityOfServiceLevel.AtMostOnce,
            true,
            TestContext.Current.CancellationToken);
        Assert.Equal(1, CountMatches(broker, "failure/#"));
    }

    [Fact]
    public async Task RestorePublishesCompleteRetainedTrie()
    {
        RecordingRetainedStore store = new();
        store.Messages["restore/one"] = Persisted("restore/one");
        store.Messages["restore/two"] = Persisted("restore/two");
        ServiceCollection services = new();
        services.AddSingleton<IMqttZeroRetainedMessageStore>(store);
        services.AddMqttZero();
        await using ServiceProvider provider = services.BuildServiceProvider();
        MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();

        await broker.StartAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, CountMatches(broker, "restore/+"));
        Assert.Equal(2, broker.GetRetainedMessages().Count);
    }

    private static int CountMatches(
        MqttZeroBroker broker,
        string filter)
    {
        List<MqttZeroRetainedMessage> matches = [];
        broker.RetainedTopicTrieForTest.Match(
            System.Text.Encoding.UTF8.GetBytes(filter),
            matches);
        return matches.Count;
    }

    private static MqttZeroPersistedRetainedMessage Persisted(
        string topic)
    {
        return new MqttZeroPersistedRetainedMessage
        {
            Topic = System.Text.Encoding.UTF8.GetBytes(topic),
            Payload = new byte[] { 1 },
            QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
        };
    }

    private sealed class RecordingRetainedStore :
        IMqttZeroRetainedMessageStore
    {
        private int _failNextSave;

        public Dictionary<string, MqttZeroPersistedRetainedMessage> Messages
        {
            get;
        } = [];

        public bool FailNextSave
        {
            get
            {
                return Volatile.Read(ref _failNextSave) != 0;
            }
            set
            {
                Volatile.Write(ref _failNextSave, value ? 1 : 0);
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
                IReadOnlyList<MqttZeroPersistedRetainedMessage>>(
                Messages.Values.ToArray());
        }

        public ValueTask SaveAsync(
            MqttZeroPersistedRetainedMessage message,
            CancellationToken cancellationToken)
        {
            if (Interlocked.Exchange(ref _failNextSave, 0) != 0)
            {
                throw new InvalidOperationException(
                    "Retained save failed.");
            }

            Messages[
                System.Text.Encoding.UTF8.GetString(message.Topic.Span)] =
                message;
            return ValueTask.CompletedTask;
        }

        public ValueTask DeleteAsync(
            string topic,
            CancellationToken cancellationToken)
        {
            Messages.Remove(topic);
            return ValueTask.CompletedTask;
        }

        public ValueTask ClearAsync(CancellationToken cancellationToken)
        {
            Messages.Clear();
            return ValueTask.CompletedTask;
        }
    }
}
