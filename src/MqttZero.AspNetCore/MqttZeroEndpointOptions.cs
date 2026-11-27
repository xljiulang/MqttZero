namespace MqttZero.AspNetCore;

/// <summary>
/// TCP 或 TLS 监听端点配置。
/// </summary>
public sealed class MqttZeroEndpointOptions
{
    /// <summary>
    /// 获取已受理套接字的选项。
    /// </summary>
    public MqttZeroAcceptedSocketOptions AcceptedSockets { get; } = new();

    /// <summary>
    /// 获取或设置静态 TLS 配置；为空表示不启用 MQTTZero 的 TLS 配置。
    /// </summary>
    public MqttZeroTlsOptions? Tls { get; set; }

    /// <summary>
    /// 获取或设置按连接决定报文分片的回调；为空表示允许分片。
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
        return AllowPacketFragmentationSelector?.Invoke(transportInfo) ?? true;
    }
}
