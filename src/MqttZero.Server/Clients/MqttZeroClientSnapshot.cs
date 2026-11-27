using System.Net;
using MqttZero.Protocol;

namespace MqttZero.Server.Clients;

/// <summary>
/// 客户端某一时刻的只读视图。
/// </summary>
public sealed class MqttZeroClientSnapshot
{
    /// <summary>
    /// Gets the client identifier.
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>Gets the user name.</summary>
    public string? UserName { get; init; }

    /// <summary>
    /// Gets the remote network endpoint.
    /// </summary>
    public EndPoint? RemoteEndPoint { get; init; }

    /// <summary>
    /// Gets a value indicating whether the connection uses TLS.
    /// </summary>
    public bool IsSecure { get; init; }

    /// <summary>
    /// Gets a value indicating whether the connection uses WebSocket.
    /// </summary>
    public bool IsWebSocket { get; init; }

    /// <summary>Gets the MQTT protocol version.</summary>
    public MqttProtocolVersion ProtocolVersion { get; init; }

    /// <summary>
    /// Gets the number of active subscriptions.
    /// </summary>
    public int SubscriptionCount { get; init; }

    /// <summary>Gets current connection statistics.</summary>
    public MqttZeroClientStatistics Statistics { get; init; } = new();
}
