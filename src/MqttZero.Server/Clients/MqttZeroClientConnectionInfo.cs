using MqttZero.Protocol;
using System.Net;
using System.Security.Cryptography.X509Certificates;

namespace MqttZero.Server.Clients;

/// <summary>
/// 连接的传输与身份信息。
/// </summary>
public sealed class MqttZeroClientConnectionInfo
{
    /// <summary>获取客户端标识。</summary>
    public required string ClientId { get; init; }
    /// <summary>获取用户名。</summary>
    public string? UserName { get; init; }
    /// <summary>获取远端地址。</summary>
    public EndPoint? RemoteEndPoint { get; init; }
    /// <summary>获取连接是否使用 TLS。</summary>
    public bool IsSecure { get; init; }
    /// <summary>获取连接是否使用 WebSocket。</summary>
    public bool IsWebSocket { get; init; }
    /// <summary>获取 MQTT 协议版本。</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; }
    /// <summary>获取客户端证书。</summary>
    public X509Certificate2? ClientCertificate { get; init; }
}
