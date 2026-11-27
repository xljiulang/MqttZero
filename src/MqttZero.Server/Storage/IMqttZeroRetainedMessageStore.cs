namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化保留消息。
/// </summary>
public interface IMqttZeroRetainedMessageStore
{
    /// <summary>获取是否启用持久化。</summary>
    bool IsEnabled { get; }
    /// <summary>加载全部保留消息。</summary>
    ValueTask<IReadOnlyList<MqttZeroPersistedRetainedMessage>> LoadAllAsync(CancellationToken cancellationToken);
    /// <summary>保存一条保留消息。</summary>
    ValueTask SaveAsync(MqttZeroPersistedRetainedMessage message, CancellationToken cancellationToken);
    /// <summary>删除一条保留消息。</summary>
    ValueTask DeleteAsync(string topic, CancellationToken cancellationToken);
    /// <summary>删除全部保留消息。</summary>
    ValueTask ClearAsync(CancellationToken cancellationToken);
}
