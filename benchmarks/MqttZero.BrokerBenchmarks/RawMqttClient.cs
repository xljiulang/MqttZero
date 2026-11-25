using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Text;

namespace MqttZero.BrokerBenchmarks;

internal sealed class RawMqttClient : IBenchmarkMqttClient
{
    private readonly ConcurrentDictionary<ushort, TaskCompletionSource<MqttPacket>>
        _pendingPackets = new();
    private readonly RawMqttClientOptions _options;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private CancellationTokenSource? _lifetime;
    private TaskCompletionSource<MqttConnAckPacket>? _connectCompletion;
    private PipeReader? _reader;
    private Task? _receiveTask;
    private TcpClient? _tcpClient;
    private PipeWriter? _writer;
    private int _connected;
    private int _disposed;
    private long _receivedMessages;
    private int _nextPacketIdentifier;

    public RawMqttClient(RawMqttClientOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.Port);
    }

    public bool IsConnected
    {
        get
        {
            return Volatile.Read(ref _connected) != 0;
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
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        if (IsConnected)
        {
            throw new InvalidOperationException(
                "The raw MQTT client is already connected.");
        }

        string effectiveClientId = string.IsNullOrEmpty(clientId)
            ? _options.ClientId ??
                throw new ArgumentException(
                    "A client identifier is required.",
                    nameof(clientId))
            : clientId;
        _tcpClient = new TcpClient();
        try
        {
            await _tcpClient.ConnectAsync(
                _options.Host,
                _options.Port,
                cancellationToken).ConfigureAwait(false);
            NetworkStream stream = _tcpClient.GetStream();
            _reader = PipeReader.Create(stream);
            _writer = PipeWriter.Create(stream);
            _lifetime = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
            _connectCompletion = CreateCompletion<MqttConnAckPacket>();
            Volatile.Write(ref _connected, 1);
            _receiveTask = ReceiveLoopAsync(_lifetime.Token);

            MqttConnectPacket packet = new()
            {
                CleanStart = true,
                ClientId = new ReadOnlySequence<byte>(
                    Encoding.UTF8.GetBytes(effectiveClientId)),
                KeepAliveSeconds = _options.KeepAliveSeconds,
                ProtocolVersion = MqttProtocolVersion.V500
            };
            await SendPacketAsync(packet, cancellationToken)
                .ConfigureAwait(false);
            MqttConnAckPacket connAck = await _connectCompletion.Task
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            if (connAck.ReasonCode != MqttReasonCode.Success)
            {
                throw new InvalidOperationException(
                    $"MQTT CONNECT failed with {connAck.ReasonCode}.");
            }
        }
        catch
        {
            await DisposeConnectionAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask SubscribeAsync(
        string topicFilter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        CancellationToken cancellationToken)
    {
        ThrowIfNotConnected();
        RejectQosTwo(qualityOfServiceLevel);
        ushort packetIdentifier = RentPacketIdentifier();
        TaskCompletionSource<MqttPacket> completion =
            CreateCompletion<MqttPacket>();
        if (!_pendingPackets.TryAdd(packetIdentifier, completion))
        {
            throw new InvalidOperationException(
                "The MQTT packet identifier is already pending.");
        }

        try
        {
            MqttSubscribePacket packet = new()
            {
                PacketIdentifier = packetIdentifier,
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>(
                            Encoding.UTF8.GetBytes(topicFilter)),
                        QualityOfServiceLevel = qualityOfServiceLevel
                    }
                ]
            };
            await SendPacketAsync(packet, cancellationToken)
                .ConfigureAwait(false);
            MqttPacket result = await completion.Task
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            MqttSubAckPacket subAck = result as MqttSubAckPacket ??
                throw new InvalidOperationException(
                    "The broker returned an unexpected packet for SUBSCRIBE.");
            if (subAck.ReasonCodes.Length == 0 ||
                ContainsFailureReasonCode(subAck.ReasonCodes.Span))
            {
                throw new InvalidOperationException(
                    "The broker rejected the benchmark subscription.");
            }
        }
        finally
        {
            _pendingPackets.TryRemove(packetIdentifier, out _);
        }
    }

    public async ValueTask PublishAsync(
        string topic,
        ReadOnlyMemory<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        CancellationToken cancellationToken)
    {
        if (qualityOfServiceLevel ==
            MqttQualityOfServiceLevel.ExactlyOnce)
        {
            throw new NotSupportedException(
                "The raw benchmark client supports QoS 0 and QoS 1 only.");
        }

        ThrowIfNotConnected();
        ushort packetIdentifier = 0;
        TaskCompletionSource<MqttPacket>? completion = null;
        if (qualityOfServiceLevel ==
            MqttQualityOfServiceLevel.AtLeastOnce)
        {
            packetIdentifier = RentPacketIdentifier();
            completion = CreateCompletion<MqttPacket>();
            if (!_pendingPackets.TryAdd(packetIdentifier, completion))
            {
                throw new InvalidOperationException(
                    "The MQTT packet identifier is already pending.");
            }
        }

        try
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>(
                    Encoding.UTF8.GetBytes(topic)),
                Payload = new ReadOnlySequence<byte>(payload),
                QualityOfServiceLevel = qualityOfServiceLevel,
                PacketIdentifier = packetIdentifier
            };
            await SendPacketAsync(packet, cancellationToken)
                .ConfigureAwait(false);
            if (completion != null)
            {
                MqttPacket result = await completion.Task
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                if (result is not MqttPubAckPacket pubAck)
                {
                    throw new InvalidOperationException(
                        "The broker returned an unexpected packet for PUBLISH.");
                }

                if ((byte)pubAck.ReasonCode >= 0x80)
                {
                    throw new InvalidOperationException(
                        $"MQTT PUBLISH failed with {pubAck.ReasonCode}.");
                }
            }
        }
        finally
        {
            if (packetIdentifier != 0)
            {
                _pendingPackets.TryRemove(packetIdentifier, out _);
            }
        }
    }

    public async ValueTask DisconnectAsync(
        CancellationToken cancellationToken)
    {
        if (!IsConnected)
        {
            return;
        }

        try
        {
            await SendPacketAsync(
                new MqttDisconnectPacket
                {
                    ReasonCode = MqttReasonCode.Success
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await DisposeConnectionAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await DisposeConnectionAsync().ConfigureAwait(false);
        _writeLock.Dispose();
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            PipeReader reader = _reader ??
                throw new InvalidOperationException(
                    "The raw MQTT reader is not initialized.");
            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult result = await reader.ReadAsync(cancellationToken)
                    .ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = result.Buffer;
                long consumedBytes = 0;
                while (true)
                {
                    ReadOnlySequence<byte> remaining =
                        buffer.Slice(consumedBytes);
                    if (remaining.IsEmpty)
                    {
                        break;
                    }

                    if (MqttPacketCodec.TryDecode(
                            remaining,
                            out MqttPacket? packet,
                            out long consumed))
                    {
                        consumedBytes += consumed;
                        await ProcessPacketAsync(
                            packet!,
                            cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    if (MqttPacketCodec.TryGetPacketLength(
                            remaining,
                            out _,
                            out bool complete) &&
                        complete)
                    {
                        throw new InvalidOperationException(
                            "The broker sent a malformed MQTT packet.");
                    }

                    break;
                }

                reader.AdvanceTo(
                    buffer.GetPosition(consumedBytes),
                    buffer.End);
                if (result.IsCompleted)
                {
                    throw new IOException(
                        "The broker closed the raw MQTT connection.");
                }
            }
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            FailPending(exception);
            _connectCompletion?.TrySetException(exception);
        }
        finally
        {
            Volatile.Write(ref _connected, 0);
        }
    }

    private async ValueTask ProcessPacketAsync(
        MqttPacket packet,
        CancellationToken cancellationToken)
    {
        switch (packet)
        {
            case MqttConnAckPacket connAck:
                _connectCompletion?.TrySetResult(connAck);
                break;
            case MqttSubAckPacket subAck:
                CompletePending(subAck.PacketIdentifier, subAck);
                break;
            case MqttPubAckPacket pubAck:
                CompletePending(pubAck.PacketIdentifier, pubAck);
                break;
            case MqttPublishPacket publish:
                Interlocked.Increment(ref _receivedMessages);
                if (publish.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce)
                {
                    await SendPacketAsync(
                        new MqttPubAckPacket
                        {
                            PacketIdentifier = publish.PacketIdentifier,
                            ReasonCode = MqttReasonCode.Success
                        },
                        cancellationToken).ConfigureAwait(false);
                }
                else if (publish.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.ExactlyOnce)
                {
                    throw new NotSupportedException(
                        "The raw benchmark client cannot receive QoS 2 PUBLISH packets.");
                }

                break;
            case MqttDisconnectPacket disconnect:
                throw new InvalidOperationException(
                    $"The broker disconnected the raw client with {disconnect.ReasonCode}.");
        }
    }

    private async ValueTask SendPacketAsync(
        MqttPacket packet,
        CancellationToken cancellationToken)
    {
        PipeWriter writer = _writer ??
            throw new InvalidOperationException(
                "The raw MQTT writer is not initialized.");
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            MqttPacketCodec.Encode(packet, writer);
            FlushResult result = await writer.FlushAsync(cancellationToken)
                .ConfigureAwait(false);
            if (result.IsCompleted)
            {
                throw new IOException(
                    "The broker closed the raw MQTT connection.");
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private ushort RentPacketIdentifier()
    {
        for (int attempt = 0; attempt < ushort.MaxValue; attempt++)
        {
            int candidate = Interlocked.Increment(ref _nextPacketIdentifier);
            ushort packetIdentifier = (ushort)candidate;
            if (packetIdentifier != 0 &&
                !_pendingPackets.ContainsKey(packetIdentifier))
            {
                return packetIdentifier;
            }
        }

        throw new InvalidOperationException(
            "No MQTT packet identifier is available.");
    }

    private void CompletePending(
        ushort packetIdentifier,
        MqttPacket packet)
    {
        if (_pendingPackets.TryGetValue(
                packetIdentifier,
                out TaskCompletionSource<MqttPacket>? completion))
        {
            completion.TrySetResult(packet);
        }
    }

    private void FailPending(Exception exception)
    {
        foreach (TaskCompletionSource<MqttPacket> completion in
            _pendingPackets.Values)
        {
            completion.TrySetException(exception);
        }
    }

    private void ThrowIfNotConnected()
    {
        if (!IsConnected)
        {
            throw new InvalidOperationException(
                "The raw MQTT client is not connected.");
        }
    }

    private static void RejectQosTwo(
        MqttQualityOfServiceLevel qualityOfServiceLevel)
    {
        if (qualityOfServiceLevel ==
            MqttQualityOfServiceLevel.ExactlyOnce)
        {
            throw new NotSupportedException(
                "The raw benchmark client supports QoS 0 and QoS 1 only.");
        }
    }

    private static bool ContainsFailureReasonCode(ReadOnlySpan<byte> reasonCodes)
    {
        for (int index = 0; index < reasonCodes.Length; index++)
        {
            if (reasonCodes[index] >= 0x80)
            {
                return true;
            }
        }

        return false;
    }

    private async ValueTask DisposeConnectionAsync()
    {
        Volatile.Write(ref _connected, 0);
        CancellationTokenSource? lifetime = _lifetime;
        if (lifetime != null)
        {
            try
            {
                await lifetime.CancelAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        if (_writer != null)
        {
            try
            {
                await _writer.CompleteAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        if (_reader != null)
        {
            try
            {
                await _reader.CompleteAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        _tcpClient?.Dispose();
        if (_receiveTask != null)
        {
            try
            {
                await _receiveTask.ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        lifetime?.Dispose();
        _lifetime = null;
        _reader = null;
        _writer = null;
        _tcpClient = null;
        _receiveTask = null;
        _connectCompletion = null;
    }

    private static TaskCompletionSource<T> CreateCompletion<T>()
    {
        return new TaskCompletionSource<T>(
            TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
