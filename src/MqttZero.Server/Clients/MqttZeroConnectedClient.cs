using MqttZero.Server.Broker;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Clients;

/// <summary>Provides connection-scoped operations for one connected client.</summary>
public sealed class MqttZeroConnectedClient
{
    private readonly MqttZeroBroker _broker;
    private readonly MqttZeroSessionState _session;
    private readonly MqttZeroClientConnection _connection;

    internal MqttZeroConnectedClient(
        MqttZeroBroker broker,
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        _broker = broker;
        _session = session;
        _connection = connection;
    }

    /// <summary>Gets the client information.</summary>
    public MqttZeroClientConnectionInfo ClientInfo
    {
        get
        {
            return _connection.ClientInfo;
        }
    }

    /// <summary>Gets session-scoped application items.</summary>
    public IDictionary<object, object?> Items
    {
        get
        {
            return _session.Items;
        }
    }

    /// <summary>Gets whether this handle is still attached.</summary>
    public bool IsConnected
    {
        get
        {
            return MqttZeroBroker.IsConnectionAttached(_session, _connection);
        }
    }

    /// <summary>Gets current connection statistics.</summary>
    public MqttZeroClientStatistics Statistics
    {
        get
        {
            return _connection.GetStatistics();
        }
    }

    /// <summary>Subscribes this client to the requested topic filters.</summary>
    public ValueTask<IReadOnlyList<MqttZeroSubscribeResult>> SubscribeAsync(
        IReadOnlyList<MqttSubscriptionRequest> subscriptions,
        uint? subscriptionIdentifier = null,
        CancellationToken cancellationToken = default)
    {
        return _broker.SubscribeConnectedClientAsync(
            _session,
            _connection,
            subscriptions,
            subscriptionIdentifier,
            cancellationToken);
    }

    /// <summary>Unsubscribes this client from the requested topic filters.</summary>
    public ValueTask<IReadOnlyList<MqttZeroUnsubscribeResult>> UnsubscribeAsync(
        IReadOnlyList<string> topicFilters,
        CancellationToken cancellationToken = default)
    {
        return _broker.UnsubscribeConnectedClientAsync(
            _session,
            _connection,
            topicFilters,
            cancellationToken);
    }

    /// <summary>Sends one PUBLISH packet only to this client.</summary>
    public ValueTask<bool> SendPublishAsync(
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken = default)
    {
        return _broker.SendPublishToClientAsync(
            _session,
            _connection,
            message,
            cancellationToken);
    }

    /// <summary>Disconnects this client.</summary>
    public ValueTask<bool> DisconnectAsync(
        MqttReasonCode reasonCode = MqttReasonCode.Success,
        CancellationToken cancellationToken = default)
    {
        return _broker.DisconnectConnectedClientAsync(
            _session,
            _connection,
            reasonCode,
            cancellationToken);
    }

    /// <summary>Gets a point-in-time client snapshot.</summary>
    public MqttZeroClientSnapshot GetSnapshot()
    {
        return MqttZeroBroker.CreateClientSnapshot(_session, _connection);
    }

    /// <summary>Resets this connection's statistics.</summary>
    public void ResetStatistics()
    {
        _connection.ResetStatistics();
    }
}
