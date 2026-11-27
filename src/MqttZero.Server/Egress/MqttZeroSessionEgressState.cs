namespace MqttZero.Server.Egress;

/// <summary>
/// 出站状态机的状态。
/// </summary>
internal enum MqttZeroSessionEgressState
{
    Idle,
    Draining,
    FlushPending,
    Stopping,
    Stopped
}
