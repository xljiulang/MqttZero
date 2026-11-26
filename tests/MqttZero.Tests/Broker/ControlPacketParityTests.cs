using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;

namespace MqttZero.Tests.Broker
{
    public sealed class ControlPacketParityTests
    {
        public static TheoryData<string, MqttReasonCode> AcknowledgementCases
        {
            get
            {
                return new TheoryData<string, MqttReasonCode>
                {
                    { "puback", MqttReasonCode.Success },
                    { "puback", MqttReasonCode.NotAuthorized },
                    { "pubrec", MqttReasonCode.Success },
                    { "pubrec", MqttReasonCode.NotAuthorized },
                    { "pubrel", MqttReasonCode.Success },
                    { "pubrel", MqttReasonCode.PacketIdentifierNotFound },
                    { "pubcomp", MqttReasonCode.Success },
                    { "pubcomp", MqttReasonCode.PacketIdentifierNotFound }
                };
            }
        }

        [Theory]
        [MemberData(nameof(AcknowledgementCases))]
        public async Task HandWrittenAcknowledgementsMatchTheCodec(
            string packetKind,
            MqttReasonCode reasonCode)
        {
            DrainCountingPipeWriter writer = new();
            MqttZeroClientConnection connection = CreateConnection(writer);
            try
            {
                await SendAcknowledgementAsync(connection, packetKind, reasonCode);
                Assert.Equal(
                    EncodeExpectedAcknowledgement(packetKind, reasonCode),
                    writer.ResetAndReadWrittenBytes());
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }

        [Fact]
        public async Task PingResponseMatchesTheCodec()
        {
            DrainCountingPipeWriter writer = new();
            MqttZeroClientConnection connection = CreateConnection(writer);
            try
            {
                await connection.SendPingRespAsync(TestContext.Current.CancellationToken);

                ArrayBufferWriter<byte> expected = new();
                MqttPacketCodec.Encode(new MqttPingRespPacket(), expected);
                Assert.Equal(expected.WrittenSpan.ToArray(), writer.ResetAndReadWrittenBytes());
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }

        [Fact]
        public async Task SubscriptionAcknowledgementsMatchTheCodec()
        {
            DrainCountingPipeWriter writer = new();
            MqttZeroClientConnection connection = CreateConnection(writer);
            try
            {
                MqttSubAckPacket subAck = new()
                {
                    PacketIdentifier = 7,
                    ReasonCodes = new byte[] { 0x00, 0x01, 0x02 }
                };
                await connection.SendSubAckAsync(
                    subAck,
                    TestContext.Current.CancellationToken);
                ArrayBufferWriter<byte> expectedSubAck = new();
                MqttPacketCodec.Encode(subAck, expectedSubAck);
                Assert.Equal(
                    expectedSubAck.WrittenSpan.ToArray(),
                    writer.ResetAndReadWrittenBytes());

                MqttUnsubAckPacket unsubAck = new()
                {
                    PacketIdentifier = 9,
                    ReasonCodes = new byte[] { 0x00, 0x11 }
                };
                await connection.SendUnsubAckAsync(
                    unsubAck,
                    TestContext.Current.CancellationToken);
                ArrayBufferWriter<byte> expectedUnsubAck = new();
                MqttPacketCodec.Encode(unsubAck, expectedUnsubAck);
                Assert.Equal(
                    expectedUnsubAck.WrittenSpan.ToArray(),
                    writer.ResetAndReadWrittenBytes());
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }

        [Fact]
        public async Task ConnAckAndDisconnectMatchTheCodec()
        {
            DrainCountingPipeWriter writer = new();
            MqttZeroClientConnection connection = CreateConnection(writer);
            try
            {
                MqttConnAckPacket connAck = new()
                {
                    SessionPresent = true,
                    ReasonCode = MqttReasonCode.Success,
                    ReceiveMaximum = 32,
                    MaximumPacketSize = 65536
                };
                await connection.SendConnAckAsync(
                    connAck,
                    TestContext.Current.CancellationToken);
                ArrayBufferWriter<byte> expectedConnAck = new();
                MqttPacketCodec.Encode(connAck, expectedConnAck);
                Assert.Equal(
                    expectedConnAck.WrittenSpan.ToArray(),
                    writer.ResetAndReadWrittenBytes());

                MqttDisconnectPacket disconnect = new()
                {
                    ReasonCode = MqttReasonCode.ProtocolError
                };
                await connection.SendDisconnectAsync(
                    disconnect,
                    TestContext.Current.CancellationToken);
                ArrayBufferWriter<byte> expectedDisconnect = new();
                MqttPacketCodec.Encode(disconnect, expectedDisconnect);
                Assert.Equal(
                    expectedDisconnect.WrittenSpan.ToArray(),
                    writer.ResetAndReadWrittenBytes());
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }

        private static MqttZeroClientConnection CreateConnection(PipeWriter writer)
        {
            return new MqttZeroClientConnection(
                writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System,
                new MqttZeroBufferPool(1024 * 1024));
        }

        private static ValueTask SendAcknowledgementAsync(
            MqttZeroClientConnection connection,
            string packetKind,
            MqttReasonCode reasonCode)
        {
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            if (packetKind == "puback")
            {
                return connection.SendPubAckAsync(1, reasonCode, cancellationToken);
            }

            if (packetKind == "pubrec")
            {
                return connection.SendPubRecAsync(1, reasonCode, cancellationToken);
            }

            if (packetKind == "pubrel")
            {
                return connection.SendPubRelAsync(1, reasonCode, cancellationToken);
            }

            return connection.SendPubCompAsync(1, reasonCode, cancellationToken);
        }

        private static byte[] EncodeExpectedAcknowledgement(
            string packetKind,
            MqttReasonCode reasonCode)
        {
            ArrayBufferWriter<byte> writer = new();
            MqttPacket packet = packetKind switch
            {
                "puback" => new MqttPubAckPacket
                {
                    PacketIdentifier = 1,
                    ReasonCode = reasonCode
                },
                "pubrec" => new MqttPubRecPacket
                {
                    PacketIdentifier = 1,
                    ReasonCode = reasonCode
                },
                "pubrel" => new MqttPubRelPacket
                {
                    PacketIdentifier = 1,
                    ReasonCode = reasonCode
                },
                _ => new MqttPubCompPacket
                {
                    PacketIdentifier = 1,
                    ReasonCode = reasonCode
                }
            };
            MqttPacketCodec.Encode(packet, writer);
            return writer.WrittenSpan.ToArray();
        }

        private sealed class DrainCountingPipeWriter : PipeWriter
        {
            private readonly Pipe _pipe = new();

            public override void Advance(int bytes)
            {
                _pipe.Writer.Advance(bytes);
            }

            public override void CancelPendingFlush()
            {
                _pipe.Writer.CancelPendingFlush();
            }

            public override void Complete(Exception? exception = null)
            {
                _pipe.Writer.Complete(exception);
            }

            public override ValueTask<FlushResult> FlushAsync(
                CancellationToken cancellationToken = default)
            {
                return _pipe.Writer.FlushAsync(cancellationToken);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                return _pipe.Writer.GetMemory(sizeHint);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return _pipe.Writer.GetSpan(sizeHint);
            }

            public byte[] ResetAndReadWrittenBytes()
            {
                FlushResult result = _pipe.Writer.FlushAsync()
                    .AsTask()
                    .GetAwaiter()
                    .GetResult();
                _ = result;
                ReadOnlySequence<byte> buffer = _pipe.Reader
                    .ReadAsync()
                    .AsTask()
                    .GetAwaiter()
                    .GetResult()
                    .Buffer;
                byte[] bytes = buffer.ToArray();
                _pipe.Reader.AdvanceTo(buffer.End);
                return bytes;
            }
        }
    }
}
