using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;

namespace MqttZero.Benchmarks;

/// <summary>
/// Measures QoS 0 and QoS 1 publish fanout.
/// </summary>
[MemoryDiagnoser]
public class FanoutBenchmarks : IDisposable
{
    private MqttZeroBroker _broker = null!;
    private MqttZeroMessageLease _lease = null!;
    private MqttZeroSessionState[] _receivers = [];
    private MqttZeroClientConnection[] _connections = [];
    private MemoryWriter[] _writers = [];
    private MqttZeroSessionState _sender = null!;
    private MqttZeroClientConnection _senderConnection = null!;
    private MemoryWriter _senderWriter = null!;

    /// <summary>
    /// Gets or sets the subscriber count.
    /// </summary>
    [Params(1, 10, 100)]
    public int Subscribers { get; set; }

    /// <summary>
    /// Gets or sets the PUBLISH QoS.
    /// </summary>
    [Params(MqttQualityOfServiceLevel.AtMostOnce, MqttQualityOfServiceLevel.AtLeastOnce)]
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// Creates broker sessions and connections.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        MqttZeroOptions options = new()
        {
            MaxInflight = Math.Max(32, Subscribers),
            MaxMessageQueueLength = Math.Max(1000, Subscribers)
        };
        _broker = MqttZeroBroker.Create(
            options,
            TimeProvider.System);
        _receivers = new MqttZeroSessionState[Subscribers];
        _connections = new MqttZeroClientConnection[Subscribers];
        _writers = new MemoryWriter[Subscribers];
        FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
            "_sessions",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        ConcurrentDictionary<string, MqttZeroSessionState> sessions =
            (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField.GetValue(_broker)!;
        for (int index = 0; index < Subscribers; index++)
        {
            MqttZeroSessionState receiver = new($"receiver-{index}", options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "hot/#"u8.ToArray(),
                QualityOfServiceLevel,
                false,
                false,
                null));
            MemoryWriter writer = new(16 * 1024);
            MqttZeroClientConnection connection = new(
                writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            receiver.Connection = connection;
            sessions.TryAdd(receiver.ClientId, receiver);
            _receivers[index] = receiver;
            _writers[index] = writer;
            _connections[index] = connection;
        }

        typeof(MqttZeroBroker).GetMethod(
            "RebuildSubscriptionRouteSnapshot",
            BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(_broker, null);
        _sender = new MqttZeroSessionState("sender");
        _senderWriter = new MemoryWriter(16 * 1024);
        _senderConnection = new MqttZeroClientConnection(
            _senderWriter,
            new IPEndPoint(IPAddress.Loopback, 0),
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        _lease = MqttZeroMessageLease.BorrowFrom(
            new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>(new byte[1024]),
            QualityOfServiceLevel,
            false,
            null,
            null);
    }

    /// <summary>
    /// Measures one publish fanout.
    /// </summary>
    /// <returns>The number of configured subscribers.</returns>
    [Benchmark]
    [SuppressMessage(
        "Reliability",
        "CA2012:ValueTask instances should not have their result directly accessed",
        Justification = "The benchmark requires a synchronous non-allocating call path.")]
    public int Route()
    {
        _senderWriter.Reset();
        for (int index = 0; index < _writers.Length; index++)
        {
            _writers[index].Reset();
        }

        ValueTask route = _broker.RoutePublishLeaseAsync(
            _sender,
            _senderConnection,
            _lease,
            CancellationToken.None);
        if (!route.IsCompletedSuccessfully)
        {
            throw new InvalidOperationException("The fanout route did not complete synchronously.");
        }

        route.GetAwaiter().GetResult();
        ResetReceiverState();
        return Subscribers;
    }

    /// <summary>
    /// Releases benchmark resources.
    /// </summary>
    public void Dispose()
    {
        _lease.Release();
        foreach (MqttZeroClientConnection connection in _connections)
        {
            connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        _senderConnection.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _broker.DisposeAsync().AsTask().GetAwaiter().GetResult();
        GC.SuppressFinalize(this);
    }

    private void ResetReceiverState()
    {
        foreach (MqttZeroSessionState receiver in _receivers)
        {
            foreach (MqttZeroQueuedMessage message in
                receiver.InflightMessages.Values)
            {
                message.Release();
            }

            receiver.InflightMessages.Clear();
            while (receiver.PendingMessages.Count > 0)
            {
                receiver.PendingMessages.Dequeue().Release();
            }
        }
    }

    private sealed class MemoryWriter : PipeWriter
    {
        private readonly byte[] _buffer;
        private int _written;

        public MemoryWriter(int capacity)
        {
            _buffer = new byte[capacity];
        }

        public override void Advance(int bytes)
        {
            _written += bytes;
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
            return _buffer.AsMemory(_written);
        }

        public override Span<byte> GetSpan(int sizeHint = 0)
        {
            return _buffer.AsSpan(_written);
        }

        public override ValueTask<FlushResult> WriteAsync(
            ReadOnlyMemory<byte> source,
            CancellationToken cancellationToken = default)
        {
            source.Span.CopyTo(_buffer.AsSpan(_written));
            _written += source.Length;
            return ValueTask.FromResult(new FlushResult(false, false));
        }

        public void Reset()
        {
            _written = 0;
        }
    }
}
