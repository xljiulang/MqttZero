using MqttZero.Server.Clients;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Retained;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

/// <summary>
/// 面向使用者的 Broker 操作接口。
/// </summary>
public interface IMqttZeroBroker
{
    /// <summary>
    /// Gets the current broker lifecycle state.
    /// </summary>
    MqttZeroBrokerState State { get; }

    /// <summary>
    /// Gets the number of active accepted transport connections.
    /// </summary>
    int ActiveConnectionCount { get; }

    /// <summary>
    /// Gets the number of connected MQTT clients.
    /// </summary>
    int ConnectedClientCount { get; }

    /// <summary>
    /// Gets the number of will messages published since broker creation.
    /// </summary>
    long PublishedWillCount { get; }

    /// <summary>
    /// Gets the maximum MQTT packet size accepted by the broker.
    /// </summary>
    int MaximumPacketSize { get; }

    /// <summary>
    /// Publishes a server-originated message to matching subscribers.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing the publish operation.</returns>
    ValueTask PublishAsync(
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a point-in-time snapshot of connected clients.
    /// </summary>
    /// <returns>The connected client snapshots.</returns>
    IReadOnlyList<MqttZeroClientSnapshot> GetClientSnapshots();

    /// <summary>
    /// Gets snapshots for all stored sessions.
    /// </summary>
    /// <returns>The session snapshots.</returns>
    IReadOnlyList<MqttZeroSessionSnapshot> GetSessionSnapshots();

    /// <summary>
    /// Gets retained message metadata snapshots.
    /// </summary>
    /// <returns>The retained message snapshots.</returns>
    IReadOnlyList<MqttZeroRetainedMessageSnapshot> GetRetainedMessageSnapshots();

    /// <summary>
    /// Tries to get the connected client handle for a client identifier.
    /// </summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="client">The connected client handle when found.</param>
    /// <returns><see langword="true"/> when the client is connected.</returns>
    bool TryGetClient(
        string clientId,
        out MqttZeroConnectedClient? client);

    /// <summary>
    /// Disconnects one connected client.
    /// </summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="reasonCode">The DISCONNECT reason code.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns><see langword="true"/> when a connected client was found.</returns>
    ValueTask<bool> DisconnectClientAsync(
        string clientId,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken = default);
}
