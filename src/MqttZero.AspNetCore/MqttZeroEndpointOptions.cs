namespace MqttZero.AspNetCore;

/// <summary>
/// TCP 或 TLS 监听端点配置。
/// </summary>
public sealed class MqttZeroEndpointOptions
{
    /// <summary>
    /// Gets the accepted socket options.
    /// </summary>
    public MqttZeroAcceptedSocketOptions AcceptedSockets { get; } = new();

    /// <summary>
    /// Gets or sets the static TLS configuration. A null value disables MQTTZero TLS setup.
    /// </summary>
    public MqttZeroTlsOptions? Tls { get; set; }

    /// <summary>
    /// Gets or sets a per-connection packet fragmentation selector. A null value allows fragmentation.
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
