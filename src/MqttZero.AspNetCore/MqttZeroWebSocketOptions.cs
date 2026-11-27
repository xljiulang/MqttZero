namespace MqttZero.AspNetCore;

/// <summary>
/// MQTT over WebSocket 端点配置。
/// </summary>
public sealed class MqttZeroWebSocketOptions
{
    /// <summary>
    /// Gets or sets a per-connection packet fragmentation selector. A null value disables fragmentation.
    /// </summary>
    public Func<MqttZeroTransportInfo, bool>? AllowPacketFragmentationSelector { get; set; }

    /// <summary>
    /// 解析Allow报文Fragmentation。
    /// </summary>
    /// <param name="transportInfo">传输Info。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    internal bool ResolveAllowPacketFragmentation(MqttZeroTransportInfo transportInfo)
    {
        ArgumentNullException.ThrowIfNull(transportInfo);
        return AllowPacketFragmentationSelector?.Invoke(transportInfo) ?? false;
    }
}
