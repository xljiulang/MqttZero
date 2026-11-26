namespace MqttZero.Server.Egress;

internal enum MqttZeroSessionEgressState
{
    Idle,
    Draining,
    FlushPending,
    Stopping,
    Stopped
}
