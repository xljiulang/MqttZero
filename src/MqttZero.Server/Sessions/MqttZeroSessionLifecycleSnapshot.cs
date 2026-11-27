using MqttZero.Server.Clients;

namespace MqttZero.Server.Sessions;

/// <summary>
/// 会话生命周期计数快照。
/// </summary>
internal sealed record MqttZeroSessionLifecycleSnapshot(
    MqttZeroClientConnection? Connection,
    bool CleanStart,
    uint ExpiryInterval,
    DateTimeOffset? DisconnectedAt,
    MqttZeroWillMessage? WillMessage,
    DateTimeOffset? WillDueAt)
{
    internal bool IsConnected
    {
        get
        {
            return Connection != null;
        }
    }
}
