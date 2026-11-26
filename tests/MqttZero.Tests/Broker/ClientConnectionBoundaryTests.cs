using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Observers;
using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero.Tests.Broker
{
    public sealed class ClientConnectionBoundaryTests
    {
        [Fact]
        public async Task PubAckOverMaximumPacketSizeDoesNotWrite()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 3);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await connection.SendPubAckAsync(
                    1,
                    MqttReasonCode.Success,
                    TestContext.Current.CancellationToken));

            Assert.Empty(writer.WrittenBytes);
        }

        [Fact]
        public async Task PingResponseOverMaximumPacketSizeDoesNotWrite()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 1);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await connection.SendPingRespAsync(
                    TestContext.Current.CancellationToken));

            Assert.Empty(writer.WrittenBytes);
        }

        [Fact]
        public async Task ConnAckTrimsOptionalProblemInformationToFitMaximumPacketSize()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 6);

            await connection.SendConnAckAsync(
                new MqttConnAckPacket
                {
                    ReasonCode = MqttReasonCode.NotAuthorized,
                    ReasonString = "too long",
                    UserProperties = [new MqttUserProperty("trace", "value")]
                },
                TestContext.Current.CancellationToken);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenBytes),
                out MqttPacket? packet,
                out _));
            MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(packet);
            Assert.Equal(MqttReasonCode.NotAuthorized, connAck.ReasonCode);
            Assert.Null(connAck.ReasonString);
            Assert.Empty(connAck.UserProperties);
        }

        [Fact]
        public async Task DisconnectTrimsOptionalProblemInformationToFitMaximumPacketSize()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 6);

            await connection.SendDisconnectAsync(
                new MqttDisconnectPacket
                {
                    ReasonCode = MqttReasonCode.ServerShuttingDown,
                    ReasonString = "too long",
                    ServerReference = "backup.example",
                    UserProperties = [new MqttUserProperty("trace", "value")]
                },
                TestContext.Current.CancellationToken);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenBytes),
                out MqttPacket? packet,
                out _));
            MqttDisconnectPacket disconnect = Assert.IsType<MqttDisconnectPacket>(packet);
            Assert.Equal(MqttReasonCode.ServerShuttingDown, disconnect.ReasonCode);
            Assert.Null(disconnect.ReasonString);
            Assert.Null(disconnect.ServerReference);
            Assert.Empty(disconnect.UserProperties);
        }

        [Fact]
        public async Task ConnAckTrimsReasonStringOnly()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 6);

            await connection.SendConnAckAsync(
                new MqttConnAckPacket
                {
                    ReasonCode = MqttReasonCode.NotAuthorized,
                    ReasonString = "too long"
                },
                TestContext.Current.CancellationToken);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenBytes),
                out MqttPacket? packet,
                out _));
            Assert.Null(Assert.IsType<MqttConnAckPacket>(packet).ReasonString);
        }

        [Fact]
        public async Task ConnAckTrimsUserPropertiesOnly()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 5);

            await connection.SendConnAckAsync(
                new MqttConnAckPacket
                {
                    ReasonCode = MqttReasonCode.NotAuthorized,
                    UserProperties = [new MqttUserProperty("trace", "value")]
                },
                TestContext.Current.CancellationToken);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenBytes),
                out MqttPacket? packet,
                out _));
            Assert.Empty(Assert.IsType<MqttConnAckPacket>(packet).UserProperties);
        }

        [Fact]
        public async Task SubAckTrimsReasonStringToFitMaximumPacketSize()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 6);

            await connection.SendSubAckAsync(
                new MqttSubAckPacket
                {
                    PacketIdentifier = 1,
                    ReasonCodes = new ReadOnlyMemory<byte>([0x00]),
                    ReasonString = "too long"
                },
                TestContext.Current.CancellationToken);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenBytes),
                out MqttPacket? packet,
                out _));
            Assert.Null(Assert.IsType<MqttSubAckPacket>(packet).ReasonString);
        }

        [Fact]
        public async Task UnsubAckTrimsUserPropertiesToFitMaximumPacketSize()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 6);

            await connection.SendUnsubAckAsync(
                new MqttUnsubAckPacket
                {
                    PacketIdentifier = 1,
                    ReasonCodes = new ReadOnlyMemory<byte>([0x00]),
                    UserProperties = [new MqttUserProperty("trace", "value")]
                },
                TestContext.Current.CancellationToken);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenBytes),
                out MqttPacket? packet,
                out _));
            Assert.Empty(Assert.IsType<MqttUnsubAckPacket>(packet).UserProperties);
        }

        [Fact]
        public async Task DisconnectTrimsReasonStringOnly()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 4);

            await connection.SendDisconnectAsync(
                new MqttDisconnectPacket
                {
                    ReasonCode = MqttReasonCode.ServerShuttingDown,
                    ReasonString = "too long"
                },
                TestContext.Current.CancellationToken);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenBytes),
                out MqttPacket? packet,
                out _));
            Assert.Null(Assert.IsType<MqttDisconnectPacket>(packet).ReasonString);
        }

        [Fact]
        public async Task DisconnectTrimsServerReferenceOnly()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 4);

            await connection.SendDisconnectAsync(
                new MqttDisconnectPacket
                {
                    ReasonCode = MqttReasonCode.ServerShuttingDown,
                    ServerReference = "backup.example"
                },
                TestContext.Current.CancellationToken);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenBytes),
                out MqttPacket? packet,
                out _));
            Assert.Null(Assert.IsType<MqttDisconnectPacket>(packet).ServerReference);
        }

        [Fact]
        public async Task DisposeIsIdempotent()
        {
            MqttZeroClientConnection connection = CreateConnection(
                new TrackingPipeWriter(),
                maximumPacketSize: 1024);

            await connection.DisposeAsync();
            await connection.DisposeAsync();
        }

        [Fact]
        public async Task TopicAliasReplacementReturnsAllBuffersOnDispose()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroClientConnection connection = new(
                new TrackingPipeWriter(),
                null,
                false,
                false,
                0,
                32,
                1024,
                TimeProvider.System,
                pool);
            connection.SetInboundTopicAlias(
                1,
                new ReadOnlySequence<byte>("first"u8.ToArray()));
            connection.SetInboundTopicAlias(
                1,
                new ReadOnlySequence<byte>("a much longer topic"u8.ToArray()));

            Assert.True(connection.TryGetInboundTopicAlias(
                1,
                out ReadOnlyMemory<byte> topic));
            Assert.Equal("a much longer topic", System.Text.Encoding.UTF8.GetString(topic.Span));
            Assert.True(pool.RentedBytes > 0);

            await connection.DisposeAsync();

            Assert.Equal(0, pool.RentedBytes);
        }

        [Fact]
        public async Task SubAckAndUnsubAckUseDirectControlEncoding()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 1024);

            await connection.SendSubAckAsync(
                new MqttSubAckPacket
                {
                    PacketIdentifier = 1,
                    ReasonCodes = new ReadOnlyMemory<byte>([0x00, 0x01])
                },
                TestContext.Current.CancellationToken);
            await connection.SendUnsubAckAsync(
                new MqttUnsubAckPacket
                {
                    PacketIdentifier = 2,
                    ReasonCodes = new ReadOnlyMemory<byte>([0x00])
                },
                TestContext.Current.CancellationToken);

            Assert.Equal(2, connection.GetStatistics().PacketsSent);
            Assert.True(writer.WrittenBytes.Length > 0);
        }

        [Fact]
        public async Task CloseAsyncPublishesDisconnectReasonAndCancelsClosedToken()
        {
            TrackingPipeWriter writer = new();
            await using MqttZeroClientConnection connection = CreateConnection(
                writer,
                maximumPacketSize: 1024);

            await connection.CloseAsync(
                MqttReasonCode.ServerShuttingDown,
                TestContext.Current.CancellationToken);

            Assert.True(connection.ClosedToken.IsCancellationRequested);
            Assert.Equal(MqttReasonCode.ServerShuttingDown, connection.CloseReasonCode);
            Assert.Equal(
                MqttZeroDisconnectType.ServerShutdown,
                connection.DisconnectType);
            Assert.NotEmpty(writer.WrittenBytes);
        }

        private static MqttZeroClientConnection CreateConnection(
            PipeWriter writer,
            uint maximumPacketSize)
        {
            return new MqttZeroClientConnection(
                writer,
                null,
                false,
                false,
                0,
                32,
                maximumPacketSize,
                TimeProvider.System);
        }

        private sealed class TrackingPipeWriter : PipeWriter
        {
            private byte[] _buffer = new byte[256];
            private int _written;

            public byte[] WrittenBytes
            {
                get
                {
                    return _buffer.AsSpan(0, _written).ToArray();
                }
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

            private void EnsureCapacity(int sizeHint)
            {
                int required = _written + Math.Max(sizeHint, 1);
                if (required <= _buffer.Length)
                {
                    return;
                }

                Array.Resize(ref _buffer, Math.Max(required, _buffer.Length * 2));
            }
        }
    }
}
