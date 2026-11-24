namespace MqttZero.Server.Storage;

/// <summary>Persists queued, outbound, and inbound QoS 2 messages.</summary>
public interface IMqttZeroMessageStore
{
    /// <summary>Gets whether persistence is enabled.</summary>
    bool IsEnabled { get; }
    /// <summary>Loads all persisted messages.</summary>
    ValueTask<IReadOnlyList<MqttZeroPersistedMessage>> LoadAllAsync(CancellationToken cancellationToken);
    /// <summary>Saves a queued message.</summary>
    ValueTask SaveQueuedAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Saves an outbound in-flight message.</summary>
    ValueTask SaveOutboundAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Saves an inbound QoS 2 message.</summary>
    ValueTask SaveInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Saves an accepted inbound dispatch message.</summary>
    ValueTask SaveInboundDispatchAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return SaveQueuedAsync(message, cancellationToken);
    }
    /// <summary>Deletes an accepted inbound dispatch message.</summary>
    ValueTask DeleteInboundDispatchAsync(
        string clientId,
        long messageId,
        CancellationToken cancellationToken)
    {
        return DeleteMessageAsync(clientId, messageId, cancellationToken);
    }
    /// <summary>Atomically promotes one queued message to outbound in-flight state.</summary>
    ValueTask TransitionQueuedToOutboundAsync(
        string clientId,
        long messageId,
        MqttZeroPersistedMessage message,
        CancellationToken cancellationToken);
    /// <summary>Deletes a queued or outbound message.</summary>
    ValueTask DeleteMessageAsync(string clientId, long messageId, CancellationToken cancellationToken);
    /// <summary>Deletes an inbound QoS 2 message.</summary>
    ValueTask DeleteInboundQos2Async(string clientId, ushort packetIdentifier, CancellationToken cancellationToken);
    /// <summary>Deletes all messages owned by one client.</summary>
    ValueTask DeleteByClientAsync(string clientId, CancellationToken cancellationToken);
    /// <summary>Atomically replaces one queued message.</summary>
    ValueTask ReplaceQueuedAsync(
        string clientId,
        long replacedMessageId,
        MqttZeroPersistedMessage message,
        CancellationToken cancellationToken);
}
