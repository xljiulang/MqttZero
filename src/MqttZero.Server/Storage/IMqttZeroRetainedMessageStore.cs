namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化保留消息。
/// </summary>
public interface IMqttZeroRetainedMessageStore
{
    /// <summary>Gets whether persistence is enabled.</summary>
    bool IsEnabled { get; }
    /// <summary>Loads all retained messages.</summary>
    ValueTask<IReadOnlyList<MqttZeroPersistedRetainedMessage>> LoadAllAsync(CancellationToken cancellationToken);
    /// <summary>Saves one retained message.</summary>
    ValueTask SaveAsync(MqttZeroPersistedRetainedMessage message, CancellationToken cancellationToken);
    /// <summary>Deletes one retained message.</summary>
    ValueTask DeleteAsync(string topic, CancellationToken cancellationToken);
    /// <summary>Deletes all retained messages.</summary>
    ValueTask ClearAsync(CancellationToken cancellationToken);
}
