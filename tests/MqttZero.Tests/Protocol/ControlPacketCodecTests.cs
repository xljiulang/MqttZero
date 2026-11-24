using MqttZero.Tests.Support;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Text;


namespace MqttZero.Tests.Protocol
{
    public sealed class ControlPacketCodecTests
    {
        [Fact]
        public void RoundTripsAcknowledgementProblemInformation()
        {
            MqttPacket[] packets =
            [
                new MqttPubAckPacket
                {
                    PacketIdentifier = 11,
                    ReasonString = "ack",
                    UserProperties = [new MqttUserProperty("stage", "puback")]
                },
                new MqttPubRecPacket
                {
                    PacketIdentifier = 12,
                    ReasonString = "received",
                    UserProperties = [new MqttUserProperty("stage", "pubrec")]
                },
                new MqttPubRelPacket
                {
                    PacketIdentifier = 13,
                    ReasonString = "released",
                    UserProperties = [new MqttUserProperty("stage", "pubrel")]
                },
                new MqttPubCompPacket
                {
                    PacketIdentifier = 14,
                    ReasonString = "complete",
                    UserProperties = [new MqttUserProperty("stage", "pubcomp")]
                }
            ];

            foreach (MqttPacket packet in packets)
            {
                byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
                Assert.True(MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(bytes),
                    out MqttPacket? decoded,
                    out _),
                    Convert.ToHexString(bytes));
                Assert.NotNull(decoded);
                Assert.Equal(packet.GetType(), decoded.GetType());
                MqttPacket value = decoded;
                string? reasonString = value switch
                {
                    MqttPubAckPacket value1 => value1.ReasonString,
                    MqttPubRecPacket value2 => value2.ReasonString,
                    MqttPubRelPacket value3 => value3.ReasonString,
                    MqttPubCompPacket value4 => value4.ReasonString,
                    _ => null
                };
                Assert.NotNull(reasonString);
            }
        }

        [Fact]
        public void RoundTripsDisconnectProblemInformation()
        {
            MqttDisconnectPacket packet = new()
            {
                ReasonString = "shutdown",
                ServerReference = "backup.example",
                UserProperties = [new MqttUserProperty("stage", "disconnect")]
            };

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? decoded,
                out _));

            MqttDisconnectPacket value = Assert.IsType<MqttDisconnectPacket>(decoded);
            Assert.Equal("shutdown", value.ReasonString);
            Assert.Equal("backup.example", value.ServerReference);
            Assert.Single(value.UserProperties);
        }

        [Fact]
        public void DecodesShortQos1PubAck()
        {
            byte[] bytes = [0x40, 0x02, 0x00, 0x2A];

            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out long consumed);

            MqttPubAckPacket pubAckPacket = Assert.IsType<MqttPubAckPacket>(packet);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal((ushort)42, pubAckPacket.PacketIdentifier);
            Assert.Equal(MqttReasonCode.Success, pubAckPacket.ReasonCode);
        }

        [Theory]
        [InlineData("4003002A00", MqttPacketType.PubAck)]
        [InlineData("5003002A00", MqttPacketType.PubRec)]
        [InlineData("6203002A00", MqttPacketType.PubRel)]
        [InlineData("7003002A00", MqttPacketType.PubComp)]
        public void DecodesAcknowledgementWithReasonButNoPropertyLength(
            string hex,
            MqttPacketType packetType)
        {
            byte[] bytes = Convert.FromHexString(hex);

            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out long consumed);

            Assert.True(decoded, hex);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal(packetType, packet!.PacketType);
            Assert.Equal(MqttReasonCode.Success, GetReasonCode(packet));
        }

        [Theory]
        [InlineData("4002002A", MqttPacketType.PubAck, MqttReasonCode.Success)]
        [InlineData("4003002A00", MqttPacketType.PubAck, MqttReasonCode.Success)]
        [InlineData("4004002A0000", MqttPacketType.PubAck, MqttReasonCode.Success)]
        [InlineData("4008002A00041F000178", MqttPacketType.PubAck, MqttReasonCode.Success)]
        [InlineData("5003002A00", MqttPacketType.PubRec, MqttReasonCode.Success)]
        [InlineData("6203002A00", MqttPacketType.PubRel, MqttReasonCode.Success)]
        [InlineData("7003002A00", MqttPacketType.PubComp, MqttReasonCode.Success)]
        public void DecodesAcknowledgementView(
            string hex,
            MqttPacketType packetType,
            MqttReasonCode reasonCode)
        {
            byte[] bytes = Convert.FromHexString(hex);
            bool decoded = MqttPacketCodec.TryDecodeAcknowledgement(
                new ReadOnlySequence<byte>(bytes),
                out MqttZeroAcknowledgementView view,
                out long consumed);

            Assert.True(decoded, hex);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal(packetType, view.PacketType);
            Assert.Equal((ushort)42, view.PacketIdentifier);
            Assert.Equal(reasonCode, view.ReasonCode);
        }

        [Fact]
        public void AcknowledgementViewDecodingDoesNotAllocateAfterWarmup()
        {
            byte[] bytes = Convert.FromHexString("4008002A00041F000178");
            ReadOnlySequence<byte> source = new(bytes);

            for (int index = 0; index < 100; index++)
            {
                Assert.True(MqttPacketCodec.TryDecodeAcknowledgement(
                    source,
                    out _,
                    out _));
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                Assert.True(MqttPacketCodec.TryDecodeAcknowledgement(
                    source,
                    out _,
                    out _));
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(0, allocated);
        }

        [Fact]
        public void DecodesBadAuthenticationMethodConnAckReason()
        {
            byte[] bytes = [0x20, 0x03, 0x00, 0x8C, 0x00];

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out _));

            Assert.Equal(
                MqttReasonCode.BadAuthenticationMethod,
                Assert.IsType<MqttConnAckPacket>(packet).ReasonCode);
        }

        [Fact]
        public void DecodesPacketTooLargeDisconnectReason()
        {
            byte[] bytes = [0xE0, 0x02, 0x95, 0x00];

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out _));

            Assert.Equal(
                MqttReasonCode.PacketTooLarge,
                Assert.IsType<MqttDisconnectPacket>(packet).ReasonCode);
        }

        [Fact]
        public void RoundTripsQos2AcknowledgementPackets()
        {
            MqttPacket[] packets =
            [
                new MqttPubRecPacket
                {
                    PacketIdentifier = 11,
                    ReasonCode = MqttReasonCode.PacketIdentifierInUse
                },
                new MqttPubRelPacket
                {
                    PacketIdentifier = 12
                },
                new MqttPubCompPacket
                {
                    PacketIdentifier = 13,
                    ReasonCode = MqttReasonCode.PacketIdentifierNotFound
                }
            ];

            foreach (MqttPacket packet in packets)
            {
                byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
                Assert.True(MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(bytes),
                    out MqttPacket? decoded,
                    out long consumed));
                Assert.Equal(bytes.Length, consumed);
                Assert.Equal(packet.PacketType, decoded!.PacketType);
            }
        }
        [Fact]
        public void ReadsPacketLengthBeforeFullPacketArrives()
        {
            byte[] bytes = [0x30, 0x09, 0x00, 0x03, (byte)'d', (byte)'e', (byte)'v'];

            bool found = MqttPacketCodec.TryGetPacketLength(
                new ReadOnlySequence<byte>(bytes),
                out long packetLength,
                out bool complete);

            Assert.True(found);
            Assert.False(complete);
            Assert.Equal(11, packetLength);
        }
        [Fact]
        public void RoundTripsDisconnectSessionExpiryOverride()
        {
            MqttDisconnectPacket packet = new()
            {
                SessionExpiryInterval = 0
            };

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out _);

            MqttDisconnectPacket disconnectPacket = Assert.IsType<MqttDisconnectPacket>(result);
            Assert.True(decoded);
            Assert.Equal(0U, disconnectPacket.SessionExpiryInterval!.Value);
        }

        [Theory]
        [InlineData(0x40)]
        [InlineData(0x50)]
        [InlineData(0x62)]
        [InlineData(0x70)]
        public void AcknowledgementDecodersRejectZeroPacketIdentifier(byte firstByte)
        {
            byte[] bytes = [firstByte, 0x02, 0x00, 0x00];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void AcknowledgementEncodersRejectZeroPacketIdentifier()
        {
            MqttPacket[] packets =
            [
                new MqttPubAckPacket(),
                new MqttPubRecPacket(),
                new MqttPubRelPacket(),
                new MqttPubCompPacket()
            ];

            foreach (MqttPacket packet in packets)
            {
                Assert.Throws<ArgumentException>(
                    () => ProtocolPacketTestHelper.Encode(packet));
            }
        }

        [Theory]
        [InlineData("2003020000")]
        [InlineData("2003018000")]
        [InlineData("2003000100")]
        public void ConnAckDecoderRejectsInvalidFlagsOrReasonCode(string hex)
        {
            byte[] bytes = Convert.FromHexString(hex);
            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Theory]
        [InlineData("900400000000")]
        [InlineData("900400010003")]
        [InlineData("9003000100")]
        [InlineData("B00400000000")]
        [InlineData("B00400010001")]
        [InlineData("B003000100")]
        public void SubscriptionAcknowledgementDecodersRejectInvalidPackets(string hex)
        {
            byte[] bytes = Convert.FromHexString(hex);
            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        private static MqttReasonCode GetReasonCode(MqttPacket packet)
        {
            return packet switch
            {
                MqttPubAckPacket pubAck => pubAck.ReasonCode,
                MqttPubRecPacket pubRec => pubRec.ReasonCode,
                MqttPubRelPacket pubRel => pubRel.ReasonCode,
                MqttPubCompPacket pubComp => pubComp.ReasonCode,
                _ => throw new InvalidOperationException(
                    $"Unexpected acknowledgement type: {packet.GetType()}.")
            };
        }
    }
}
