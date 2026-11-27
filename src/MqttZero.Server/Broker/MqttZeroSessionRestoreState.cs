using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

/// <summary>
/// 连接失败时需要恢复的会话与出站状态。
/// </summary>
internal readonly struct MqttZeroSessionRestoreState
{
    /// <summary>
    /// 初始化 MqttZeroSessionRestoreState 的新实例。
    /// </summary>
    /// <param name="replacedSession">replaced会话。</param>
    /// <param name="oldConnection">old连接。</param>
    /// <param name="previousPumpSession">前一个Pump会话。</param>
    /// <param name="previousPump">前一个Pump。</param>
    /// <param name="previousPumpStopped">前一个PumpStopped。</param>
    /// <param name="previousCleanStart">前一个CleanStart。</param>
    /// <param name="previousExpiryInterval">前一个Expiry间隔。</param>
    /// <param name="previousDisconnectedAt">前一个DisconnectedAt。</param>
    /// <param name="previousWillMessage">前一个遗嘱消息。</param>
    /// <param name="previousWillDueAt">前一个遗嘱DueAt。</param>
    public MqttZeroSessionRestoreState(
        MqttZeroSessionState? replacedSession,
        MqttZeroClientConnection? oldConnection,
        MqttZeroSessionState? previousPumpSession,
        MqttZeroSessionEgress? previousPump,
        bool previousPumpStopped,
        bool previousCleanStart,
        uint previousExpiryInterval,
        DateTimeOffset? previousDisconnectedAt,
        MqttZeroWillMessage? previousWillMessage,
        DateTimeOffset? previousWillDueAt)
    {
        ReplacedSession = replacedSession;
        OldConnection = oldConnection;
        PreviousPumpSession = previousPumpSession;
        PreviousPump = previousPump;
        PreviousPumpStopped = previousPumpStopped;
        PreviousCleanStart = previousCleanStart;
        PreviousExpiryInterval = previousExpiryInterval;
        PreviousDisconnectedAt = previousDisconnectedAt;
        PreviousWillMessage = previousWillMessage;
        PreviousWillDueAt = previousWillDueAt;
    }

    /// <summary>
    /// Replaced会话。
    /// </summary>
    public MqttZeroSessionState? ReplacedSession { get; }

    /// <summary>
    /// Old连接。
    /// </summary>
    public MqttZeroClientConnection? OldConnection { get; }

    /// <summary>
    /// 前一个Pump会话。
    /// </summary>
    public MqttZeroSessionState? PreviousPumpSession { get; }

    /// <summary>
    /// 前一个Pump。
    /// </summary>
    public MqttZeroSessionEgress? PreviousPump { get; }

    /// <summary>
    /// 前一个PumpStopped。
    /// </summary>
    public bool PreviousPumpStopped { get; }

    /// <summary>
    /// 前一个CleanStart。
    /// </summary>
    public bool PreviousCleanStart { get; }

    /// <summary>
    /// 前一个Expiry间隔。
    /// </summary>
    public uint PreviousExpiryInterval { get; }

    /// <summary>
    /// 前一个DisconnectedAt。
    /// </summary>
    public DateTimeOffset? PreviousDisconnectedAt { get; }

    /// <summary>
    /// 前一个遗嘱消息。
    /// </summary>
    public MqttZeroWillMessage? PreviousWillMessage { get; }

    /// <summary>
    /// 前一个遗嘱DueAt。
    /// </summary>
    public DateTimeOffset? PreviousWillDueAt { get; }
}
