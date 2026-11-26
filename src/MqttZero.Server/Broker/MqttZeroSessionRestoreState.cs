using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

/// <summary>
/// Captures the session and egress state that a failed CONNECT must restore.
/// </summary>
internal readonly struct MqttZeroSessionRestoreState
{
    public MqttZeroSessionRestoreState(
        MqttZeroSessionState? replacedSession,
        MqttZeroClientConnection? oldConnection,
        MqttZeroSessionState? previousPumpSession,
        MqttZeroEgressActor? previousPump,
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

    public MqttZeroSessionState? ReplacedSession { get; }

    public MqttZeroClientConnection? OldConnection { get; }

    public MqttZeroSessionState? PreviousPumpSession { get; }

    public MqttZeroEgressActor? PreviousPump { get; }

    public bool PreviousPumpStopped { get; }

    public bool PreviousCleanStart { get; }

    public uint PreviousExpiryInterval { get; }

    public DateTimeOffset? PreviousDisconnectedAt { get; }

    public MqttZeroWillMessage? PreviousWillMessage { get; }

    public DateTimeOffset? PreviousWillDueAt { get; }
}
