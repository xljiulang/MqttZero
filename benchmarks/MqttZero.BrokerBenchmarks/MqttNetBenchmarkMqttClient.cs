using MQTTnet;
using MqttZero.Protocol;
using MqttQos = MQTTnet.Protocol.MqttQualityOfServiceLevel;

namespace MqttZero.BrokerBenchmarks;

internal sealed class MqttNetBenchmarkMqttClient : IBenchmarkMqttClient
{
    private readonly IMqttClient _client;
    private readonly int _port;
    private long _receivedMessages;

    public MqttNetBenchmarkMqttClient(
        IMqttClient client,
        int port)
    {
        _client = client;
        _port = port;
        _client.ApplicationMessageReceivedAsync += _ =>
        {
            Interlocked.Increment(ref _receivedMessages);
            return Task.CompletedTask;
        };
    }

    public bool IsConnected
    {
        get
        {
            return _client.IsConnected;
        }
    }

    public long ReceivedMessageCount
    {
        get
        {
            return Interlocked.Read(ref _receivedMessages);
        }
    }

    public async ValueTask ConnectAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        MqttClientOptions options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", _port)
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
            .WithClientId(clientId)
            .WithCleanStart()
            .Build();
        await _client.ConnectAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SubscribeAsync(
        string topicFilter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        CancellationToken cancellationToken)
    {
        MqttClientSubscribeOptions options =
            new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(
                    topicFilter,
                    ToMqttNetQos(qualityOfServiceLevel))
                .Build();
        await _client.SubscribeAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask PublishAsync(
        string topic,
        ReadOnlyMemory<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        CancellationToken cancellationToken)
    {
        MqttApplicationMessage message = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(payload.ToArray())
            .WithQualityOfServiceLevel(ToMqttNetQos(qualityOfServiceLevel))
            .Build();
        await _client.PublishAsync(message, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisconnectAsync(CancellationToken cancellationToken)
    {
        if (_client.IsConnected)
        {
            await _client.DisconnectAsync(
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }

    private static MqttQos ToMqttNetQos(
        MqttQualityOfServiceLevel qualityOfServiceLevel)
    {
        return qualityOfServiceLevel switch
        {
            MqttQualityOfServiceLevel.AtMostOnce => MqttQos.AtMostOnce,
            MqttQualityOfServiceLevel.AtLeastOnce => MqttQos.AtLeastOnce,
            MqttQualityOfServiceLevel.ExactlyOnce => MqttQos.ExactlyOnce,
            _ => throw new ArgumentOutOfRangeException(
                nameof(qualityOfServiceLevel))
        };
    }
}
