using System.Net;
using MqttZero.Protocol;

namespace MqttZero.Server.Clients;

/// <summary>
/// 客户端某一时刻的只读视图。
/// </summary>
public sealed class MqttZeroClientSnapshot
{
    /// <summary>
    /// 获取客户端标识。
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>获取用户名。</summary>
    public string? UserName { get; init; }

    /// <summary>
    /// 获取远端网络地址。
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

    /// <summary>获取 MQTT 协议版本。</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; }

    /// <summary>
    /// 获取当前生效的订阅数量。
    /// </summary>
    public int SubscriptionCount { get; init; }

    /// <summary>获取当前连接统计。</summary>
    public MqttZeroClientStatistics Statistics { get; init; } = new();
}
