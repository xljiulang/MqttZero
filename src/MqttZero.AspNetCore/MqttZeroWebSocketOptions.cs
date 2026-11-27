namespace MqttZero.AspNetCore;

/// <summary>
/// MQTT over WebSocket 端点配置。
/// </summary>
public sealed class MqttZeroWebSocketOptions
{
    /// <summary>
    /// 获取或设置按连接决定报文分片的回调；为空表示禁止分片。
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
