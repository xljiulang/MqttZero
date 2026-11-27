using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server;
using MqttZero.Server.Clients;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;

namespace MqttZero.Tests.Broker
{
    public sealed class MessageExpiryTests
    {
        [Fact]
        public async Task ForwardedPublishUsesRemainingAbsoluteExpiry()
        {
            DateTimeOffset now = new(2026, 11, 24, 0, 0, 0, TimeSpan.Zero);
            FakeTimeProvider timeProvider = new(now);
            CapturingPipeWriter writer = new(4096);
            MqttZeroClientConnection connection = new(
                writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                4096,
                timeProvider);
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("expiry/topic"u8.ToArray()),
                new ReadOnlySequence<byte>(
                    new byte[] { 0x01, 0x01, 0x02, 0x00, 0x00, 0x00, 0x3C }),
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
            try
            {
                lease.SetExpiresAt(now.AddSeconds(5));
                await connection.TrySendPublishAsync(
                    lease,
                    MqttQualityOfServiceLevel.AtMostOnce,
                    false,
                    null,
                    0,
                    false,
                    TestContext.Current.CancellationToken);

                Assert.True(MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(writer.WrittenMemory),
                    out MqttPacket? packet,
                    out _));
                MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(packet);
                Assert.Equal(5U, publish.Properties.MessageExpiryInterval);
                Assert.Equal(
                    MqttPayloadFormatIndicator.Utf8,
                    publish.Properties.PayloadFormatIndicator);
            }
            finally
            {
                lease.Release();
                await connection.DisposeAsync();
            }
        }

        private sealed class FakeTimeProvider : TimeProvider
        {
            private readonly DateTimeOffset _utcNow;

            public FakeTimeProvider(DateTimeOffset utcNow)
            {
                _utcNow = utcNow;
            }

            public override DateTimeOffset GetUtcNow()
            {
                return _utcNow;
            }
        }

        private sealed class CapturingPipeWriter : PipeWriter
        {
            private byte[] _buffer;
            private int _written;

            public CapturingPipeWriter(int capacity)
            {
                _buffer = new byte[capacity];
            }

            public ReadOnlyMemory<byte> WrittenMemory
            {
                get
                {
                    return _buffer.AsMemory(0, _written);
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
                EnsureCapacity(_written + Math.Max(sizeHint, 1));
                return _buffer.AsMemory(_written);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                EnsureCapacity(_written + Math.Max(sizeHint, 1));
                return _buffer.AsSpan(_written);
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                EnsureCapacity(_written + source.Length);
                source.Span.CopyTo(_buffer.AsSpan(_written));
                _written += source.Length;
                return ValueTask.FromResult(new FlushResult(false, false));
            }

            private void EnsureCapacity(int required)
            {
                if (required <= _buffer.Length)
                {
                    return;
                }

                int newLength = Math.Max(required, _buffer.Length * 2);
                Array.Resize(ref _buffer, newLength);
            }
        }
    }
}
