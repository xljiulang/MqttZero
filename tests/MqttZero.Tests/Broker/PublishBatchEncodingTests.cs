using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero.Tests.Broker;

public sealed class PublishBatchEncodingTests
{
    [Fact(Timeout = 5_000)]
    public async Task EncodeAndSendBatchPathsProduceIdenticalBytes()
    {
        List<MqttZeroQueuedMessage> messages = CreateMessages();
        try
        {
            CapturingPipeWriter encodeWriter = new();
            await using MqttZeroClientConnection encodeConnection =
                CreateConnection(encodeWriter);
            int encodedBytes =
                await encodeConnection.EncodePublishBatchAsync(
                messages,
                TestContext.Current.CancellationToken);
            bool encoded = encodedBytes > 0;
            byte[] expected = encodeWriter.WrittenBytes.ToArray();

            CapturingPipeWriter sendWriter = new();
            await using MqttZeroClientConnection sendConnection =
                CreateConnection(sendWriter);
            await sendConnection.SendPublishBatchAsync(
                messages,
                TestContext.Current.CancellationToken);
            byte[] actual = sendWriter.WrittenBytes.ToArray();

            Assert.True(encoded);
            Assert.Equal(expected.Length, encodedBytes);
            Assert.Equal(expected, actual);
            Assert.Equal(1, sendWriter.FlushCount);
        }
        finally
        {
            for (int index = 0; index < messages.Count; index++)
            {
                messages[index].Release();
            }
        }
    }

    [Fact]
    public void ExpiringMessageKeepsCachedEncodedLength()
    {
        const uint MaximumPacketSize = 1024 * 1024;
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>("cache/topic"u8.ToArray()),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>(new byte[16]),
            MqttQualityOfServiceLevel.AtLeastOnce,
            retain: false,
            messageExpiryInterval: 30,
            subscriptionIdentifier: null);
        MqttZeroQueuedMessage message =
            MqttZeroQueuedMessage.RentWithIdentifiers(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                retain: false,
                ReadOnlyMemory<uint>.Empty,
                expiresAt: DateTimeOffset.UtcNow.AddSeconds(30));
        lease.Release();
        try
        {
            Assert.False(message.IsEncodedLengthValidFor(MaximumPacketSize));

            message.SetEncodedLength(42, MaximumPacketSize);

            Assert.True(message.IsEncodedLengthValidFor(MaximumPacketSize));
            Assert.False(
                message.IsEncodedLengthValidFor(MaximumPacketSize + 1));
        }
        finally
        {
            message.Release();
        }
    }

    [Fact]
    public void MessageExpiryValueDoesNotChangeEncodedLength()
    {
        int hourRemaining = GetEncodedLength(3600);
        int expired = GetEncodedLength(0);
        int withoutExpiry = GetEncodedLength(null);

        Assert.Equal(hourRemaining, expired);
        Assert.Equal(hourRemaining - 5, withoutExpiry);
    }

    private static int GetEncodedLength(uint? remainingExpiry)
    {
        MqttZeroPublishView view = new(
            new ReadOnlySequence<byte>("length/topic"u8.ToArray()),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>(new byte[16]),
            MqttQualityOfServiceLevel.AtMostOnce,
            0,
            false,
            false,
            remainingExpiry,
            null,
            ReadOnlyMemory<uint>.Empty,
            null);
        return MqttPublishPacketCodec.GetEncodedLength(view);
    }

    private static MqttZeroClientConnection CreateConnection(
        PipeWriter writer)
    {
        return new MqttZeroClientConnection(
            writer,
            remoteEndPoint: null,
            isSecure: false,
            isWebSocket: false,
            keepAliveSeconds: 0,
            receiveMaximum: 32,
            maximumPacketSize: 1024 * 1024,
            timeProvider: TimeProvider.System,
            allowPacketFragmentation: true);
    }

    private static List<MqttZeroQueuedMessage> CreateMessages()
    {
        List<MqttZeroQueuedMessage> messages = [];
        AddMessage(
            messages,
            "batch/one",
            ReadOnlySequence<byte>.Empty,
            "payload-one",
            MqttQualityOfServiceLevel.AtMostOnce,
            retain: false,
            packetIdentifier: 0,
            subscriptionIdentifiers: ReadOnlyMemory<uint>.Empty);
        AddMessage(
            messages,
            "batch/two",
            new ReadOnlySequence<byte>([0x01, 0x01]),
            "payload-two",
            MqttQualityOfServiceLevel.AtLeastOnce,
            retain: true,
            packetIdentifier: 7,
            subscriptionIdentifiers: new uint[] { 11, 13 });
        AddMessage(
            messages,
            "batch/three",
            ReadOnlySequence<byte>.Empty,
            "payload-three",
            MqttQualityOfServiceLevel.ExactlyOnce,
            retain: false,
            packetIdentifier: 9,
            subscriptionIdentifiers: new uint[] { 17 });
        return messages;
    }

    private static void AddMessage(
        List<MqttZeroQueuedMessage> messages,
        string topic,
        ReadOnlySequence<byte> properties,
        string payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        ushort packetIdentifier,
        ReadOnlyMemory<uint> subscriptionIdentifiers)
    {
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>(
                System.Text.Encoding.UTF8.GetBytes(topic)),
            properties,
            new ReadOnlySequence<byte>(
                System.Text.Encoding.UTF8.GetBytes(payload)),
            qualityOfServiceLevel,
            retain,
            null,
            null);
        MqttZeroQueuedMessage message =
            MqttZeroQueuedMessage.RentWithIdentifiers(
                lease,
                qualityOfServiceLevel,
                retain,
                subscriptionIdentifiers,
                expiresAt: null);
        lease.Release();
        message.PacketIdentifier = packetIdentifier;
        messages.Add(message);
    }

    private sealed class CapturingPipeWriter : PipeWriter
    {
        private readonly ArrayBufferWriter<byte> _buffer = new(
            1024);

        public int FlushCount { get; private set; }

        public ReadOnlyMemory<byte> WrittenBytes
        {
            get
            {
                return _buffer.WrittenMemory;
            }
        }

        public override void Advance(int bytes)
        {
            _buffer.Advance(bytes);
        }

        public override void CancelPendingFlush()
        {
        }

        public override void Complete(Exception? exception = null)
        {
        }

        public override ValueTask CompleteAsync(
            Exception? exception = null)
        {
            return ValueTask.CompletedTask;
        }

        public override ValueTask<FlushResult> FlushAsync(
            CancellationToken cancellationToken = default)
        {
            FlushCount++;
            return ValueTask.FromResult(new FlushResult(false, false));
        }

        public override Memory<byte> GetMemory(int sizeHint = 0)
        {
            return _buffer.GetMemory(sizeHint);
        }

        public override Span<byte> GetSpan(int sizeHint = 0)
        {
            return _buffer.GetSpan(sizeHint);
        }

        public override ValueTask<FlushResult> WriteAsync(
            ReadOnlyMemory<byte> source,
            CancellationToken cancellationToken = default)
        {
            source.Span.CopyTo(_buffer.GetSpan(source.Length));
            _buffer.Advance(source.Length);
            return ValueTask.FromResult(new FlushResult(false, false));
        }
    }
}
