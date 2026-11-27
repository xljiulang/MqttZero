namespace MqttZero.Server.Scheduling;

/// <summary>
/// Identifies what a scheduled broker deadline expires.
/// </summary>
internal enum MqttZeroDeadlineKind
{
    KeepAlive,
    Will,
    SessionExpiry,
    MessageExpiry,
    RetainedExpiry
}
