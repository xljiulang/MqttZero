namespace MqttZero.Server.Clients;

/// <summary>
/// 连接统计。
/// </summary>
public sealed class MqttZeroClientStatistics
{
    /// <summary>Gets the connection timestamp.</summary>
    public DateTimeOffset ConnectedAt { get; init; }
    /// <summary>Gets the last packet receive timestamp.</summary>
    public DateTimeOffset LastPacketReceivedAt { get; init; }
    /// <summary>Gets the received packet count.</summary>
    public long PacketsReceived { get; init; }
    /// <summary>Gets the sent packet count.</summary>
    public long PacketsSent { get; init; }
    /// <summary>Gets the received byte count.</summary>
    public long BytesReceived { get; init; }
    /// <summary>Gets the sent byte count.</summary>
    public long BytesSent { get; init; }
    /// <summary>Gets the received application-message count.</summary>
    public long MessagesReceived { get; init; }
    /// <summary>Gets the sent application-message count.</summary>
    public long MessagesSent { get; init; }
}
