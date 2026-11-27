namespace MqttZero.Server.Clients;

/// <summary>
/// 连接统计。
/// </summary>
public sealed class MqttZeroClientStatistics
{
    /// <summary>获取连接时间戳。</summary>
    public DateTimeOffset ConnectedAt { get; init; }
    /// <summary>获取最后一次收到报文的时间戳。</summary>
    public DateTimeOffset LastPacketReceivedAt { get; init; }
    /// <summary>获取已接收报文数。</summary>
    public long PacketsReceived { get; init; }
    /// <summary>获取已发送报文数。</summary>
    public long PacketsSent { get; init; }
    /// <summary>获取已接收字节数。</summary>
    public long BytesReceived { get; init; }
    /// <summary>获取已发送字节数。</summary>
    public long BytesSent { get; init; }
    /// <summary>获取已接收的应用消息数。</summary>
    public long MessagesReceived { get; init; }
    /// <summary>获取已发送的应用消息数。</summary>
    public long MessagesSent { get; init; }
}
