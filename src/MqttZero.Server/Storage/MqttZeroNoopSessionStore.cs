namespace MqttZero.Server.Storage;

/// <summary>
/// 默认不落盘的会话存储。
/// </summary>
internal sealed class MqttZeroNoopSessionStore : IMqttZeroSessionStore
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
    public ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(CancellationToken cancellationToken)
    {
        return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedSession>>([]);
    }

    /// <summary>
    /// 保存。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask SaveAsync(MqttZeroPersistedSession session, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// 删除。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask DeleteAsync(string clientId, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
