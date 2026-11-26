namespace MqttZero.Protocol;

/// <summary>
/// Builds MQTT fixed-header first bytes.
/// </summary>
internal static class MqttPacketHeader
{
    /// <summary>
    /// Gets the fixed-header first byte for a control packet type.
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
    /// Gets the fixed-header first byte for a PUBLISH packet.
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

    private static bool RequiresAcknowledgedFlags(MqttPacketType packetType)
    {
        return packetType is MqttPacketType.PubRel
            or MqttPacketType.Subscribe
            or MqttPacketType.Unsubscribe;
    }
}
