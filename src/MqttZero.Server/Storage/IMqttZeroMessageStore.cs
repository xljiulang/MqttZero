namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化排队、出站与入站 QoS 2 消息。
/// </summary>
public interface IMqttZeroMessageStore
{
    /// <summary>获取是否启用持久化。</summary>
    bool IsEnabled { get; }
    /// <summary>加载全部持久化消息。</summary>
    ValueTask<IReadOnlyList<MqttZeroPersistedMessage>> LoadAllAsync(CancellationToken cancellationToken);
    /// <summary>保存一条排队消息。</summary>
    ValueTask SaveQueuedAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>保存一条出站在途消息。</summary>
    ValueTask SaveOutboundAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>保存一条入站 QoS 2 消息。</summary>
    ValueTask SaveInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>保存一条已受理的入站待分发消息。</summary>
    ValueTask SaveInboundDispatchAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return SaveQueuedAsync(message, cancellationToken);
    }
    /// <summary>删除一条已受理的入站待分发消息。</summary>
    ValueTask DeleteInboundDispatchAsync(
        string clientId,
        long messageId,
        CancellationToken cancellationToken)
    {
        return DeleteMessageAsync(clientId, messageId, cancellationToken);
    }
    /// <summary>把一条排队消息原子地提升为出站在途状态。</summary>
    ValueTask TransitionQueuedToOutboundAsync(
        string clientId,
        long messageId,
        MqttZeroPersistedMessage message,
        CancellationToken cancellationToken);
    /// <summary>删除一条排队或在途消息。</summary>
    ValueTask DeleteMessageAsync(string clientId, long messageId, CancellationToken cancellationToken);
    /// <summary>删除一条入站 QoS 2 消息。</summary>
    ValueTask DeleteInboundQos2Async(string clientId, ushort packetIdentifier, CancellationToken cancellationToken);
    /// <summary>删除某个客户端拥有的全部消息。</summary>
    ValueTask DeleteByClientAsync(string clientId, CancellationToken cancellationToken);
    /// <summary>原子地替换一条排队消息。</summary>
    ValueTask ReplaceQueuedAsync(
        string clientId,
        long replacedMessageId,
        MqttZeroPersistedMessage message,
        CancellationToken cancellationToken);
}
