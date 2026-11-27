namespace MqttZero.Server.Scheduling;

/// <summary>
/// 到期项的分类。
/// </summary>
internal enum MqttZeroDeadlineKind
{
    KeepAlive,
    Will,
    SessionExpiry,
    MessageExpiry,
    RetainedExpiry
}
