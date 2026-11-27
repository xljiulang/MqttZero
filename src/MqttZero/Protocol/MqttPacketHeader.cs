namespace MqttZero.Protocol;

/// <summary>
/// 按报文类型构造固定报头首字节。
/// </summary>
internal static class MqttPacketHeader
{
    /// <summary>
    /// 获取控制报文类型的固定报头首字节。
    /// </summary>
    /// <param name="packetType">The control packet type.</param>
    /// <returns>The first fixed-header byte.</returns>
    public static byte GetFirstByte(MqttPacketType packetType)
    {
        byte firstByte = (byte)((byte)packetType << 4);
        if (RequiresAcknowledgedFlags(packetType))
        {
            firstByte |= 0x02;
        }

        return firstByte;
    }

    /// <summary>
    /// 获取 PUBLISH 报文的固定报头首字节。
    /// </summary>
    /// <param name="duplicate">Whether the duplicate flag is set.</param>
    /// <param name="qualityOfServiceLevel">The quality of service level.</param>
    /// <param name="retain">Whether the retain flag is set.</param>
    /// <returns>The first fixed-header byte.</returns>
    public static byte GetPublishFirstByte(
        bool duplicate,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain)
    {
        byte firstByte = (byte)((byte)MqttPacketType.Publish << 4);
        if (duplicate)
        {
            firstByte |= 0x08;
        }

        firstByte |= (byte)((byte)qualityOfServiceLevel << 1);
        if (retain)
        {
            firstByte |= 0x01;
        }

        return firstByte;
    }

    /// <summary>
    /// 需要已确认标志。
    /// </summary>
    /// <param name="packetType">报文类型。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private static bool RequiresAcknowledgedFlags(MqttPacketType packetType)
    {
        return packetType is MqttPacketType.PubRel
            or MqttPacketType.Subscribe
            or MqttPacketType.Unsubscribe;
    }
}
