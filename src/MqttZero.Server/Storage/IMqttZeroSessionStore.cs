namespace MqttZero.Server.Storage;

/// <summary>
/// 持久化会话元数据与订阅。
/// </summary>
public interface IMqttZeroSessionStore
{
    /// <summary>获取是否启用持久化。</summary>
    bool IsEnabled { get; }
    /// <summary>加载全部持久化会话。</summary>
    ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(CancellationToken cancellationToken);
    /// <summary>保存一个会话。</summary>
    ValueTask SaveAsync(MqttZeroPersistedSession session, CancellationToken cancellationToken);
    /// <summary>删除一个会话。</summary>
    ValueTask DeleteAsync(string clientId, CancellationToken cancellationToken);
}
