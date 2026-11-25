namespace MqttZero.Server.Egress;

internal enum MqttZeroEgressActorState
{
    Idle,
    Draining,
    FlushPending,
    Stopping,
    Stopped
}
