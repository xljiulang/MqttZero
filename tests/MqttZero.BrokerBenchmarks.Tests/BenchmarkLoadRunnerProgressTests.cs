using MqttZero.Protocol;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkLoadRunnerProgressTests
{
    [Fact]
    public async Task RunReportsConnectionWarmupAndMeasurementStages()
    {
        SimulatedMqttState state = new();
        SimulatedMqttClientFactory factory = new(state);
        BenchmarkRunOptions options = new(
            Subscribers: 1,
            Publishers: 1,
            Qos: 0,
            PayloadSize: 32,
            WarmupSeconds: 0,
            MeasurementSeconds: 1,
            MaxPendingMessages: 1024);
        List<BenchmarkRunProgress> progress = [];

        _ = await BenchmarkLoadRunner.RunAsync(
            port: 1883,
            options,
            factory,
            CancellationToken.None,
            progress.Add);

        Assert.Contains(
            progress,
            static update => update.Stage == "connected");
        Assert.Contains(
            progress,
            static update => update.Stage == "warmup-complete");
        Assert.Contains(
            progress,
            static update => update.Stage == "measurement-start");
        Assert.Contains(
            progress,
            static update => update.Stage == "measurement-complete");
        Assert.Contains(
            progress,
            static update => update.Stage == "measurement-drained");
    }

    private sealed class SimulatedMqttClientFactory : IBenchmarkMqttClientFactory
    {
        private readonly SimulatedMqttState _state;
        private int _created;

        public SimulatedMqttClientFactory(SimulatedMqttState state)
        {
            _state = state;
        }

        public IBenchmarkMqttClient Create()
        {
            bool isSubscriber = _created++ == 0;
            return new SimulatedMqttClient(
                _state,
                isSubscriber);
        }
    }

    private sealed class SimulatedMqttState
    {
        public long _receivedMessages;
    }

    private sealed class SimulatedMqttClient : IBenchmarkMqttClient
    {
        private readonly bool _isSubscriber;
        private readonly SimulatedMqttState _state;

        public SimulatedMqttClient(
            SimulatedMqttState state,
            bool isSubscriber)
        {
            _state = state;
            _isSubscriber = isSubscriber;
        }

        public bool IsConnected
        {
            get
            {
                return true;
            }
        }

        public BenchmarkDeliveryTracker Delivery { get; } = new();

        public long ReceivedMessageCount
        {
            get
            {
                return _isSubscriber
                    ? Interlocked.Read(ref _state._receivedMessages)
                    : 0;
            }
        }

        public ValueTask ConnectAsync(
            string clientId,
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask DisconnectAsync(
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask PublishAsync(
            string topic,
            ReadOnlyMemory<byte> payload,
            MqttQualityOfServiceLevel qualityOfServiceLevel,
            CancellationToken cancellationToken,
            uint? messageExpiryInterval = null)
        {
            await Task.Delay(1, cancellationToken);
            Interlocked.Increment(ref _state._receivedMessages);
        }

        public ValueTask SubscribeAsync(
            string topicFilter,
            MqttQualityOfServiceLevel qualityOfServiceLevel,
            CancellationToken cancellationToken)
        {
            return ValueTask.CompletedTask;
        }
    }
}
