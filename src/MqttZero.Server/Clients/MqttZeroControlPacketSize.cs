using MqttZero.Protocol;

namespace MqttZero.Server.Clients;

/// <summary>
/// Holds the computed property, body and encoded lengths of one outbound control packet.
/// </summary>
internal readonly struct MqttZeroControlPacketSize
{
    private MqttZeroControlPacketSize(int propertiesLength, int bodyLength)
    {
        PropertiesLength = propertiesLength;
        BodyLength = bodyLength;
        EncodedLength =
            1 +
            MqttZeroWireFormat.GetVariableByteIntegerSize(bodyLength) +
            bodyLength;
    }

    public int PropertiesLength { get; }

    public int BodyLength { get; }

    public int EncodedLength { get; }

    public static MqttZeroControlPacketSize ForReasonCodeList(
        int propertiesLength,
        int reasonCodeCount)
    {
        return new MqttZeroControlPacketSize(
            propertiesLength,
            2 +
                MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
                propertiesLength +
                reasonCodeCount);
    }

    public static MqttZeroControlPacketSize ForConnAck(int propertiesLength)
    {
        return new MqttZeroControlPacketSize(
            propertiesLength,
            2 +
                MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
                propertiesLength);
    }

    public static MqttZeroControlPacketSize ForDisconnect(
        int propertiesLength,
        bool includeDetail)
    {
        if (!includeDetail)
        {
            return new MqttZeroControlPacketSize(0, 0);
        }

        return new MqttZeroControlPacketSize(
            propertiesLength,
            1 +
                MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
                propertiesLength);
    }
}
