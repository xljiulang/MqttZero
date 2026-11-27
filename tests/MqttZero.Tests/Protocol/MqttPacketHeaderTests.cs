using MqttZero.Protocol;

namespace MqttZero.Tests.Protocol
{
    public sealed class MqttPacketHeaderTests
    {
        [Theory]
        [InlineData((byte)MqttPacketType.Connect, (byte)0x10)]
        [InlineData((byte)MqttPacketType.ConnAck, (byte)0x20)]
        [InlineData((byte)MqttPacketType.Publish, (byte)0x30)]
        [InlineData((byte)MqttPacketType.PubAck, (byte)0x40)]
        [InlineData((byte)MqttPacketType.PubRec, (byte)0x50)]
        [InlineData((byte)MqttPacketType.PubRel, (byte)0x62)]
        [InlineData((byte)MqttPacketType.PubComp, (byte)0x70)]
        [InlineData((byte)MqttPacketType.Subscribe, (byte)0x82)]
        [InlineData((byte)MqttPacketType.SubAck, (byte)0x90)]
        [InlineData((byte)MqttPacketType.Unsubscribe, (byte)0xA2)]
        [InlineData((byte)MqttPacketType.UnsubAck, (byte)0xB0)]
        [InlineData((byte)MqttPacketType.PingReq, (byte)0xC0)]
        [InlineData((byte)MqttPacketType.PingResp, (byte)0xD0)]
        [InlineData((byte)MqttPacketType.Disconnect, (byte)0xE0)]
        public void ControlPacketFirstByteMatchesWireFormat(
            byte packetType,
            byte expected)
        {
            Assert.Equal(expected, MqttPacketHeader.GetFirstByte((MqttPacketType)packetType));
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
