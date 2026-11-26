using System.IO.Pipelines;
using System.Net;
using System.Security.Cryptography.X509Certificates;

namespace MqttZero.Server.Clients;

/// <summary>
/// Describes the transport resources and metadata for one accepted MQTTZero connection.
/// </summary>
public sealed class MqttZeroTransportConnection
{
    /// <summary>
    /// Gets the inbound transport pipe.
    /// </summary>
    public required PipeReader Input { get; init; }

    /// <summary>
    /// Gets the outbound transport pipe.
    /// </summary>
    public required PipeWriter Output { get; init; }

    /// <summary>
    /// Gets the optional remote network endpoint.
    /// </summary>
    public EndPoint? RemoteEndPoint { get; init; }

    /// <summary>
    /// Gets a value indicating whether the connection uses TLS.
    /// </summary>
    public bool IsSecure { get; init; }

    /// <summary>
    /// Gets a value indicating whether the connection uses WebSocket transport.
    /// </summary>
    public bool IsWebSocket { get; init; }

    /// <summary>
    /// Gets the optional client certificate supplied by the transport.
    /// </summary>
    public X509Certificate2? ClientCertificate { get; init; }

    /// <summary>
    /// Gets a value indicating whether outbound PUBLISH packets may span multiple writes.
    /// </summary>
    public bool AllowPacketFragmentation { get; init; } = true;
}
