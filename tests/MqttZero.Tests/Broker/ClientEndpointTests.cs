using MqttZero.Server;
using MqttZero.Server.Clients;
using MqttZero.Tests.Support;
using MqttZero.Memory;
using MqttZero.Protocol;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;

namespace MqttZero.Tests.Broker
{
    public sealed class ClientEndpointTests
    {
        [Fact]
        public void ClientSnapshotPreservesTypedRemoteEndPoint()
        {
            DnsEndPoint remoteEndPoint = new("broker.example", 1883);

            MqttZeroClientSnapshot snapshot = new()
            {
                ClientId = "typed-client",
                RemoteEndPoint = remoteEndPoint
            };

            Assert.Same(remoteEndPoint, snapshot.RemoteEndPoint);
        }

        [Fact]
        public void ClientSnapshotPreservesUnixDomainSocketEndPoint()
        {
            UnixDomainSocketEndPoint remoteEndPoint = new("mqttzero.sock");

            MqttZeroClientSnapshot snapshot = new()
            {
                ClientId = "unix-client",
                RemoteEndPoint = remoteEndPoint
            };

            Assert.Same(remoteEndPoint, snapshot.RemoteEndPoint);
        }

        [Fact]
        public async Task ClientConnectionPreservesTypedRemoteEndPoint()
        {
            IPEndPoint remoteEndPoint = new(IPAddress.Loopback, 1883);
            await using MqttZeroClientConnection connection = new(
                new AllocationPipeWriter(1024),
                remoteEndPoint,
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);

            Assert.Same(remoteEndPoint, connection.RemoteEndPoint);
        }

        [Fact]
        public async Task PubAckWithReasonEncodesPropertyLength()
        {
            CapturingPipeWriter writer = new(16);
            await using MqttZeroClientConnection connection = new(
                writer,
                null,
                false,
                false,
                0,
                32,
                1024,
                TimeProvider.System);

            await connection.SendPubAckAsync(
                0x1234,
                MqttReasonCode.NotAuthorized,
                TestContext.Current.CancellationToken);

            Assert.Equal(
                new byte[] { 0x40, 0x04, 0x12, 0x34, 0x87, 0x00 },
                writer.WrittenBytes);
        }

        [Fact]
        public async Task ConnectionStatisticsIncludePublishAndControlPackets()
        {
            CapturingPipeWriter writer = new(256);
            await using MqttZeroClientConnection connection = new(
                writer,
                null,
                false,
                false,
                0,
                32,
                1024,
                TimeProvider.System);
            using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("a"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("b"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);

            await connection.TrySendPublishAsync(
                lease,
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                0,
                false,
                TestContext.Current.CancellationToken);
            await connection.SendPingRespAsync(TestContext.Current.CancellationToken);
            await connection.SendPubAckAsync(
                1,
                MqttReasonCode.NotAuthorized,
                TestContext.Current.CancellationToken);

            MqttZeroClientStatistics statistics = connection.GetStatistics();
            Assert.Equal(3, statistics.PacketsSent);
            Assert.Equal(1, statistics.MessagesSent);
            Assert.Equal(writer.WrittenBytes.Length, statistics.BytesSent);
        }

        private sealed class CapturingPipeWriter : PipeWriter
        {
            private readonly byte[] _buffer;
            private int _written;

            public CapturingPipeWriter(int capacity)
            {
                _buffer = new byte[capacity];
            }

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
        }
    }
}
