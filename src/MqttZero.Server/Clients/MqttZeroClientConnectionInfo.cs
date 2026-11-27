using MqttZero.Protocol;
using System.Net;
using System.Security.Cryptography.X509Certificates;

namespace MqttZero.Server.Clients;

/// <summary>
/// 连接的传输与身份信息。
/// </summary>
public sealed class MqttZeroClientConnectionInfo
{
    /// <summary>Gets the client identifier.</summary>
    public required string ClientId { get; init; }
    /// <summary>Gets the user name.</summary>
    public string? UserName { get; init; }
    /// <summary>Gets the remote endpoint.</summary>
    public EndPoint? RemoteEndPoint { get; init; }
    /// <summary>Gets whether the connection uses TLS.</summary>
    public bool IsSecure { get; init; }
    /// <summary>Gets whether the connection uses WebSocket.</summary>
    public bool IsWebSocket { get; init; }
    /// <summary>Gets the MQTT protocol version.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; }
    /// <summary>Gets the client certificate.</summary>
    public X509Certificate2? ClientCertificate { get; init; }
}
