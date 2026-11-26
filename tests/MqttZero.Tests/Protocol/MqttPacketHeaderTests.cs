using MqttZero.Protocol;

namespace MqttZero.Tests.Protocol
{
    public sealed class MqttPacketHeaderTests
    {
        [Theory]
        [InlineData(MqttPacketType.Connect, 0x10)]
        [InlineData(MqttPacketType.ConnAck, 0x20)]
        [InlineData(MqttPacketType.Publish, 0x30)]
        [InlineData(MqttPacketType.PubAck, 0x40)]
        [InlineData(MqttPacketType.PubRec, 0x50)]
        [InlineData(MqttPacketType.PubRel, 0x62)]
        [InlineData(MqttPacketType.PubComp, 0x70)]
        [InlineData(MqttPacketType.Subscribe, 0x82)]
        [InlineData(MqttPacketType.SubAck, 0x90)]
        [InlineData(MqttPacketType.Unsubscribe, 0xA2)]
        [InlineData(MqttPacketType.UnsubAck, 0xB0)]
        [InlineData(MqttPacketType.PingReq, 0xC0)]
        [InlineData(MqttPacketType.PingResp, 0xD0)]
        [InlineData(MqttPacketType.Disconnect, 0xE0)]
        public void ControlPacketFirstByteMatchesWireFormat(
            MqttPacketType packetType,
            byte expected)
        {
            Assert.Equal(expected, MqttPacketHeader.GetFirstByte(packetType));
        }

        [Theory]
        [InlineData(false, MqttQualityOfServiceLevel.AtMostOnce, false, 0x30)]
        [InlineData(false, MqttQualityOfServiceLevel.AtMostOnce, true, 0x31)]
        [InlineData(false, MqttQualityOfServiceLevel.AtLeastOnce, false, 0x32)]
        [InlineData(false, MqttQualityOfServiceLevel.AtLeastOnce, true, 0x33)]
        [InlineData(false, MqttQualityOfServiceLevel.ExactlyOnce, false, 0x34)]
        [InlineData(false, MqttQualityOfServiceLevel.ExactlyOnce, true, 0x35)]
        [InlineData(true, MqttQualityOfServiceLevel.AtMostOnce, false, 0x38)]
        [InlineData(true, MqttQualityOfServiceLevel.AtMostOnce, true, 0x39)]
        [InlineData(true, MqttQualityOfServiceLevel.AtLeastOnce, false, 0x3A)]
        [InlineData(true, MqttQualityOfServiceLevel.AtLeastOnce, true, 0x3B)]
        [InlineData(true, MqttQualityOfServiceLevel.ExactlyOnce, false, 0x3C)]
        [InlineData(true, MqttQualityOfServiceLevel.ExactlyOnce, true, 0x3D)]
        public void PublishFirstByteEncodesFlags(
            bool duplicate,
            MqttQualityOfServiceLevel qualityOfServiceLevel,
            bool retain,
            byte expected)
        {
            Assert.Equal(
                expected,
                MqttPacketHeader.GetPublishFirstByte(
                    duplicate,
                    qualityOfServiceLevel,
                    retain));
        }
    }
}
