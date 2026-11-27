using MqttZero.Protocol;

namespace MqttZero.Server.Clients;

/// <summary>
/// 控制报文的属性、正文与总长度。
/// </summary>
internal readonly struct MqttZeroControlPacketSize
{
    /// <summary>
    /// 初始化 MqttZeroControlPacketSize 的新实例。
    /// </summary>
    /// <param name="propertiesLength">属性长度。</param>
    /// <param name="bodyLength">正文长度。</param>
    private MqttZeroControlPacketSize(int propertiesLength, int bodyLength)
    {
        PropertiesLength = propertiesLength;
        BodyLength = bodyLength;
        EncodedLength =
            1 +
            MqttZeroWireFormat.GetVariableByteIntegerSize(bodyLength) +
            bodyLength;
    }

    /// <summary>
    /// 属性长度。
    /// </summary>
    public int PropertiesLength { get; }

    /// <summary>
    /// 正文长度。
    /// </summary>
    public int BodyLength { get; }

    /// <summary>
    /// Encoded长度。
    /// </summary>
    public int EncodedLength { get; }

    /// <summary>
    /// 原因代码List。
    /// </summary>
    /// <param name="propertiesLength">属性长度。</param>
    /// <param name="reasonCodeCount">原因代码数量。</param>
    /// <returns>对应的结果。</returns>
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

    /// <summary>
    /// Conn确认。
    /// </summary>
    /// <param name="propertiesLength">属性长度。</param>
    /// <returns>对应的结果。</returns>
    public static MqttZeroControlPacketSize ForConnAck(int propertiesLength)
    {
        return new MqttZeroControlPacketSize(
            propertiesLength,
            2 +
                MqttZeroWireFormat.GetVariableByteIntegerSize(propertiesLength) +
                propertiesLength);
    }

    /// <summary>
    /// 断开。
    /// </summary>
    /// <param name="propertiesLength">属性长度。</param>
    /// <param name="includeDetail">对应的 includeDetail。</param>
    /// <returns>对应的结果。</returns>
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
