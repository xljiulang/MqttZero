using MqttZero.Server.Clients;

namespace MqttZero.Server.Observers;

/// <summary>
/// 客户端连接建立事件。
/// </summary>
public sealed class MqttZeroClientConnectedContext
{
    /// <summary>获取已连接客户端的操作句柄。</summary>
    public required MqttZeroConnectedClient ConnectedClient { get; init; }
    /// <summary>获取是否恢复了持久会话。</summary>
    public bool SessionPresent { get; init; }
}
