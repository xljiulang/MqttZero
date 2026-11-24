using MqttZero.Protocol;
using System.Buffers;
using System.Text;

namespace MqttZero.Tests.Protocol
{
    public sealed class MqttProtocolBoundaryTests
    {
        [Theory]
        [InlineData("")]
        [InlineData("a\u0000b")]
        [InlineData("a/+")]
        [InlineData("a/#")]
        [InlineData("a+")]
        public void TopicNameValidatorRejectsInvalidNames(string topic)
        {
            Assert.False(
                MqttZeroProtocolValidator.IsValidTopicName(
                    new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(topic))));
        }

        [Theory]
        [InlineData("")]
        [InlineData("a\u0000b")]
        [InlineData("a/#/b")]
        [InlineData("a+")]
        [InlineData("$share//devices")]
        [InlineData("$share/group")]
        [InlineData("$share/group/")]
        public void TopicFilterValidatorRejectsInvalidFilters(string filter)
        {
            Assert.False(
                MqttZeroProtocolValidator.IsValidTopicFilter(
                    new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(filter))));
        }

        [Theory]
        [InlineData("devices/+/temperature")]
        [InlineData("devices/#")]
        [InlineData("$share/group/devices/+")]
        [InlineData("/")]
        [InlineData("a//b")]
        public void TopicFilterValidatorAcceptsValidFilters(string filter)
        {
            Assert.True(
                MqttZeroProtocolValidator.IsValidTopicFilter(
                    new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(filter))));
        }

        [Fact]
        public void TopicNameValidatorRejectsInvalidUtf8()
        {
            Assert.False(
                MqttZeroProtocolValidator.IsValidTopicName(
                    new ReadOnlySequence<byte>([0xC0, 0xAF])));
        }

        [Fact]
        public void PubRecWithReasonAndEmptyPropertiesDecodes()
        {
            byte[] bytes = [0x50, 0x04, 0x00, 0x01, 0x91, 0x00];

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out _));
            MqttPubRecPacket pubRec = Assert.IsType<MqttPubRecPacket>(packet);
            Assert.Equal(MqttReasonCode.PacketIdentifierInUse, pubRec.ReasonCode);
            Assert.Null(pubRec.ReasonString);
            Assert.Empty(pubRec.UserProperties);
        }

        [Fact]
        public void DisconnectWithReasonRequiresPropertyLength()
        {
            byte[] bytes = [0xE0, 0x01, 0x95];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void PubRelShortFormatDecodes()
        {
            byte[] bytes = [0x62, 0x02, 0x00, 0x01];

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out _));
            MqttPubRelPacket pubRel = Assert.IsType<MqttPubRelPacket>(packet);
            Assert.Equal((ushort)1, pubRel.PacketIdentifier);
            Assert.Equal(MqttReasonCode.Success, pubRel.ReasonCode);
        }

        [Theory]
        [InlineData("60020001")]
        [InlineData("620100")]
        [InlineData("700100")]
        public void MalformedQos2AcknowledgementsAreRejected(string hex)
        {
            byte[] bytes = Convert.FromHexString(hex);
            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }
    }
}
