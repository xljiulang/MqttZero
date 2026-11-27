namespace MqttZero.Server.Storage;

/// <summary>
/// 默认不落盘的消息存储。
/// </summary>
internal sealed class MqttZeroNoopMessageStore : IMqttZeroMessageStore
{
    /// <summary>
    /// IsEnabled 的值。
    /// </summary>
    public bool IsEnabled
    {
        get
        {
            return false;
        }
    }
    /// <summary>
    /// 加载全部。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    public ValueTask<IReadOnlyList<MqttZeroPersistedMessage>> LoadAllAsync(CancellationToken cancellationToken)
    {
        return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedMessage>>([]);
    }

    /// <summary>
    /// 保存Queued。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SaveQueuedAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 保存出站。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SaveOutboundAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 保存入站Qos2。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SaveInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// TransitionQueuedTo出站。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="messageId">消息Id。</param>
    /// <param name="message">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask TransitionQueuedToOutboundAsync(
        string clientId,
        long messageId,
        MqttZeroPersistedMessage message,
        CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 删除消息。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="messageId">消息Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask DeleteMessageAsync(
        string clientId,
        long messageId,
        CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 删除入站Qos2。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask DeleteInboundQos2Async(string clientId, ushort packetIdentifier, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 删除客户端。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask DeleteByClientAsync(string clientId, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 替换Queued。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="replacedMessageId">replaced消息Id。</param>
    /// <param name="message">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask ReplaceQueuedAsync(
        string clientId,
        long replacedMessageId,
        MqttZeroPersistedMessage message,
        CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
