namespace MqttZero.Server.Storage;

/// <summary>
/// 默认不落盘的保留消息存储。
/// </summary>
internal sealed class MqttZeroNoopRetainedMessageStore : IMqttZeroRetainedMessageStore
{
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
    public ValueTask<IReadOnlyList<MqttZeroPersistedRetainedMessage>> LoadAllAsync(CancellationToken cancellationToken)
    {
        return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedRetainedMessage>>([]);
    }

    /// <summary>
    /// 保存。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SaveAsync(MqttZeroPersistedRetainedMessage message, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 删除。
    /// </summary>
    /// <param name="topic">主题。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask DeleteAsync(string topic, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 清空。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
