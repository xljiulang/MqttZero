using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using MQTTnet.Adapter;
using MQTTnet.Formatter;
using MQTTnet.Formatter.V5;
using MqttNetPublishPacket = MQTTnet.Packets.MqttPublishPacket;

namespace MqttZero.Benchmarks;

/// <summary>
/// Measures the deterministic MQTTZero in-memory publish pipeline.
/// </summary>
[MemoryDiagnoser]
public class MemoryTransportBenchmarks : IDisposable
{
    private MqttZeroBroker _broker = null!;
    private ReadOnlySequence<byte> _encodedPublish;
    private MqttZeroClientConnection _receiverConnection = null!;
    private MqttZeroSessionState _receiver = null!;
    private MemoryPipeWriter _receiverWriter = null!;
    private MqttZeroSessionState _sender = null!;
    private MqttBufferWriter _mqttNetWriter = null!;
    private MqttV5PacketEncoder _mqttNetEncoder = null!;
    private MqttV5PacketDecoder _mqttNetDecoder = null!;
    private ReceivedMqttPacket _mqttNetReceived;

    /// <summary>
    /// Creates broker, subscriber and encoded PUBLISH fixtures.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _broker = MqttZeroBroker.Create(
            new MqttZeroOptions
            {
                EventLoopShardCount = 1
            },
            TimeProvider.System);
        _broker.StartAsync().AsTask().GetAwaiter().GetResult();
        _receiver = _broker.CreateSessionState("receiver");
        _receiver.Subscriptions.Add(new MqttZeroSubscription(
            "hot/#"u8.ToArray(),
            MqttQualityOfServiceLevel.AtMostOnce,
            false,
            false,
            null));
        _receiverWriter = new MemoryPipeWriter(16 * 1024);
        _receiverConnection = new MqttZeroClientConnection(
            _receiverWriter,
            new IPEndPoint(IPAddress.Loopback, 0),
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        _receiver.Connection = _receiverConnection;

        FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
            "_sessions",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        ConcurrentDictionary<string, MqttZeroSessionState> sessions =
            (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField
                .GetValue(_broker)!;
        sessions.TryAdd(_receiver.ClientId, _receiver);
        _broker.ResetRouteEpochFromSessions();

        _sender = _broker.CreateSessionState("sender");
        MqttPublishPacket packet = new()
        {
            TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
            Payload = new ReadOnlySequence<byte>(new byte[8192])
        };
        ArrayBufferWriter<byte> writer = new();
        MqttPacketCodec.Encode(packet, writer);
        _encodedPublish = new ReadOnlySequence<byte>(writer.WrittenMemory);

        _mqttNetWriter = new MqttBufferWriter(16 * 1024, 16 * 1024);
        _mqttNetEncoder = new MqttV5PacketEncoder(_mqttNetWriter);
        _mqttNetDecoder = new MqttV5PacketDecoder();
        MqttNetPublishPacket mqttNetPacket = new()
        {
            Topic = "hot/topic",
            Payload = new ReadOnlySequence<byte>(new byte[8192])
        };
        MqttPacketBuffer encodedNet = _mqttNetEncoder.Encode(mqttNetPacket);
        byte[] mqttNetBytes = encodedNet.Packet.AsMemory().ToArray()
            .Concat(encodedNet.Payload.ToArray())
            .ToArray();
        byte fixedHeader = mqttNetBytes[0];
        int bodyOffset = 1;
        int multiplier = 1;
        int remainingLength = 0;
        byte encoded;
        do
        {
            encoded = mqttNetBytes[bodyOffset++];
            remainingLength += (encoded & 0x7F) * multiplier;
            multiplier *= 128;
        }
        while ((encoded & 0x80) != 0);

        _mqttNetReceived = new ReceivedMqttPacket(
            fixedHeader,
            new ArraySegment<byte>(mqttNetBytes, bodyOffset, mqttNetBytes.Length - bodyOffset),
            bodyOffset + remainingLength);
    }

    /// <summary>
    /// Releases the broker and memory transport.
    /// </summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        Dispose();
    }

    /// <summary>
    /// Measures one QoS 0 publish through decode, lease, route and encode.
    /// </summary>
    /// <returns>The number of consumed input bytes.</returns>
    [Benchmark]
    [SuppressMessage(
        "Reliability",
        "CA2012:ValueTask instances should not have their result directly accessed",
        Justification = "The benchmark requires a synchronous non-allocating call path.")]
    public int PublishQos0()
    {
        _receiverWriter.Reset();
        if (!MqttPublishPacketCodec.TryDecode(
            _encodedPublish,
            out MqttZeroPublishView view,
            out long consumed))
        {
            throw new InvalidOperationException("The benchmark PUBLISH could not be decoded.");
        }

        MqttZeroMessageLease lease = _broker.CreateMessageLease(_receiverConnection, view);
        try
        {
            ValueTask route = _broker.RoutePublishLeaseAsync(
                _sender,
                _receiverConnection,
                lease,
                CancellationToken.None);
            if (!route.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException("The benchmark route did not complete synchronously.");
            }

            route.GetAwaiter().GetResult();
            return (int)consumed;
        }
        finally
        {
            lease.Release();
        }
    }

    /// <summary>
    /// Measures MQTTnet decode, topic match and encode for the same payload.
    /// </summary>
    /// <returns>The encoded packet length.</returns>
    [Benchmark(Baseline = true)]
    public int MqttNetDecodeMatchEncode()
    {
        MqttNetPublishPacket packet =
            (MqttNetPublishPacket)_mqttNetDecoder.Decode(_mqttNetReceived);
        bool matches = MQTTnet.MqttTopicFilterComparer.Compare(
            packet.Topic,
            "hot/#") == MQTTnet.MqttTopicFilterCompareResult.IsMatch;
        _mqttNetWriter.Reset(0);
        MqttPacketBuffer encoded = _mqttNetEncoder.Encode(packet);
        return encoded.Length + (matches ? 1 : 0);
    }

    /// <summary>
    /// Releases broker and transport resources.
    /// </summary>
    public void Dispose()
    {
        _receiverConnection.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.DisposeAsync().AsTask().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }

    private sealed class MemoryPipeWriter : PipeWriter
    {
        private byte[] _buffer;
        private int _written;

        public MemoryPipeWriter(int capacity)
        {
            _buffer = new byte[capacity];
        }

        public override void Advance(int bytes)
        {
            _written += bytes;
            EnsureCapacity(0);
        }

        public override void CancelPendingFlush()
        {
        }

        public override void Complete(Exception? exception = null)
        {
        }

        public override ValueTask CompleteAsync(Exception? exception = null)
        {
            return ValueTask.CompletedTask;
        }

        public override ValueTask<FlushResult> FlushAsync(
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(new FlushResult(false, false));
        }

        public override Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.AsMemory(_written);
        }

        public override Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.AsSpan(_written);
        }

        public override ValueTask<FlushResult> WriteAsync(
            ReadOnlyMemory<byte> source,
            CancellationToken cancellationToken = default)
        {
            EnsureCapacity(source.Length);
            source.Span.CopyTo(_buffer.AsSpan(_written));
            _written += source.Length;
            return ValueTask.FromResult(new FlushResult(false, false));
        }

        public void Reset()
        {
            _written = 0;
        }

        private void EnsureCapacity(int sizeHint)
        {
            int required = _written + Math.Max(sizeHint, 1);
            if (required <= _buffer.Length)
            {
                return;
            }

            int capacity = _buffer.Length * 2;
            while (capacity < required)
            {
                capacity *= 2;
            }

            Array.Resize(ref _buffer, capacity);
        }
    }
}
