using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Retained;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements broker management and server-originated publishing.
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// Gets a point-in-time snapshot of connected clients.
    /// </summary>
    /// <returns>The connected client snapshots.</returns>
    public IReadOnlyList<MqttZeroClientSnapshot> GetClients()
    {
        List<MqttZeroClientSnapshot> clients = [];
        foreach (MqttZeroSessionState session in _sessions.Values)
        {
            MqttZeroClientConnection? connection;
            int subscriptionCount;
            lock (session.SyncRoot)
            {
                connection = session.Connection;
                subscriptionCount = session.Subscriptions.Count;
            }

            if (connection == null)
            {
                continue;
            }

            clients.Add(new MqttZeroClientSnapshot
            {
                ClientId = session.ClientId,
                UserName = connection.ClientInfo.UserName,
                RemoteEndPoint = connection.RemoteEndPoint,
                IsSecure = connection.IsSecure,
                IsWebSocket = connection.IsWebSocket,
                ProtocolVersion = connection.ClientInfo.ProtocolVersion,
                SubscriptionCount = subscriptionCount,
                Statistics = connection.GetStatistics()
            });
        }

        return clients;
    }

    /// <summary>
    /// Gets snapshots for all stored sessions.
    /// </summary>
    /// <returns>The session snapshots.</returns>
    public IReadOnlyList<MqttZeroSessionSnapshot> GetSessions()
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        List<MqttZeroSessionSnapshot> snapshots = [];
        foreach (MqttZeroSessionState session in _sessions.Values)
        {
            snapshots.Add(session.CreateSnapshot(now));
        }

        return snapshots;
    }

    /// <summary>
    /// Gets retained message metadata snapshots.
    /// </summary>
    /// <returns>The retained message snapshots.</returns>
    public IReadOnlyList<MqttZeroRetainedMessageSnapshot> GetRetainedMessages()
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        List<MqttZeroRetainedMessageSnapshot> messages = [];
        foreach (KeyValuePair<string, MqttZeroRetainedMessage> pair in _retainedMessages)
        {
            MqttZeroRetainedMessage message = pair.Value;
            if (message.ExpiresAt.HasValue && message.ExpiresAt.Value <= now)
            {
                RemoveRetainedMessage(pair.Key);
                continue;
            }

            messages.Add(new MqttZeroRetainedMessageSnapshot
            {
                Topic = Encoding.UTF8.GetString(message.Topic.Span),
                PayloadLength = message.Payload.Length,
                QualityOfServiceLevel = message.QualityOfServiceLevel
            });
        }

        return messages;
    }

    /// <summary>
    /// Publishes a server-originated message to matching subscribers.
    /// </summary>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing the publish operation.</returns>
    public async ValueTask PublishAsync(
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(message.Topic);
        if (State != MqttZeroBrokerState.Running)
        {
            throw new InvalidOperationException("The MQTTZero broker is not running.");
        }

        ReadOnlySequence<byte> topic = new(Encoding.UTF8.GetBytes(message.Topic));
        if (!MqttZeroProtocolValidator.IsValidTopicName(topic))
        {
            throw new ArgumentException("The MQTT topic name is invalid.", nameof(message));
        }

        if (message.Properties?.PayloadFormatIndicator ==
                MqttPayloadFormatIndicator.Utf8 &&
            !MqttZeroProtocolValidator.IsValidUtf8(message.Payload))
        {
            throw new ArgumentException("The MQTT payload is not valid UTF-8.", nameof(message));
        }

        MqttPublishPacket packet = new()
        {
            TopicName = topic,
            Payload = message.Payload,
            Properties = message.Properties ?? MqttPublishProperties.Empty,
            QualityOfServiceLevel = message.QualityOfServiceLevel,
            Retain = message.Retain
        };
        ReadOnlySequence<byte> properties = MqttPublishPropertiesCodec.Encode(
            packet.Properties);
        MqttZeroPublishView view = new(
            topic,
            properties,
            message.Payload,
            message.QualityOfServiceLevel,
            0,
            false,
            message.Retain,
            packet.Properties.MessageExpiryInterval,
            null,
            null);
        if (MqttPublishPacketCodec.GetEncodedLength(view) > _options.MaxPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the broker maximum packet size.");
        }

        await RoutePublishAsync(null, packet, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Gets the maximum MQTT packet size accepted by the broker.
    /// </summary>
    public int MaximumPacketSize
    {
        get
        {
            return _options.MaxPacketSize;
        }
    }

    /// <summary>
    /// Disconnects one connected client.
    /// </summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="reasonCode">The DISCONNECT reason code.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns><see langword="true"/> when a connected client was found.</returns>
    public async ValueTask<bool> DisconnectClientAsync(
        string clientId,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        if (!_sessions.TryGetValue(clientId, out MqttZeroSessionState? session))
        {
            return false;
        }

        MqttZeroClientConnection? connection;
        MqttZeroEgressActor? pump;
        lock (session.SyncRoot)
        {
            connection = session.Connection;
            pump = session.Egress;
            session.Egress = null;
        }

        if (connection == null)
        {
            return false;
        }

        await StopEgressAsync(pump).ConfigureAwait(false);
        await connection.CloseAsync(reasonCode, cancellationToken).ConfigureAwait(false);
        return true;
    }
}
