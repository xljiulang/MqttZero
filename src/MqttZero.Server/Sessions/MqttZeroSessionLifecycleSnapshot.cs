using MqttZero.Server.Clients;

namespace MqttZero.Server.Sessions;

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
