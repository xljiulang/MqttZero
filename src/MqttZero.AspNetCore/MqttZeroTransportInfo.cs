using System.Net;

namespace MqttZero.AspNetCore;

/// <summary>
/// 受理阶段可用的传输信息。
/// </summary>
public sealed class MqttZeroTransportInfo
{
    /// <summary>
    /// 获取远端地址。
    /// </summary>
    public EndPoint? RemoteEndPoint { get; init; }

    /// <summary>
    /// 获取连接是否使用 TLS。
    /// </summary>
    public bool IsSecure { get; init; }

    /// <summary>
    /// 获取连接是否使用 WebSocket。
    /// </summary>
    public bool IsWebSocket { get; init; }
}
