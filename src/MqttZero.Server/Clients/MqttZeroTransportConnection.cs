using System.IO.Pipelines;
using System.Net;
using System.Security.Cryptography.X509Certificates;

namespace MqttZero.Server.Clients;

/// <summary>
/// 一条已受理连接的传输资源。
/// </summary>
public sealed class MqttZeroTransportConnection
{
    /// <summary>
    /// 获取入站传输管道。
    /// </summary>
    public required PipeReader Input { get; init; }

    /// <summary>
    /// 获取出站传输管道。
    /// </summary>
    public required PipeWriter Output { get; init; }

    /// <summary>
    /// 获取可选的远端网络地址。
    /// </summary>
    public EndPoint? RemoteEndPoint { get; init; }

    /// <summary>
    /// 获取连接是否使用 TLS。
    /// </summary>
    public bool IsSecure { get; init; }

    /// <summary>
    /// 获取连接是否使用 WebSocket 传输。
    /// </summary>
    public bool IsWebSocket { get; init; }

    /// <summary>
    /// 获取传输层提供的可选客户端证书。
    /// </summary>
    public X509Certificate2? ClientCertificate { get; init; }

    /// <summary>
    /// 获取出站 PUBLISH 是否允许跨多次写入。
    /// </summary>
    public bool AllowPacketFragmentation { get; init; } = true;
}
