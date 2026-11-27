using MqttZero.Memory;
using MqttZero.Server.Broker;
using MqttZero.Server.Observers;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroSessionEgressServices
{
    private readonly Func<
        string,
        long,
        CancellationToken,
        ValueTask> _deleteMessageAsync;
    private readonly Func<
        MqttZeroSessionState,
        MqttZeroMessageLease,
        MqttZeroMessageDropReason,
        CancellationToken,
        ValueTask> _notifyDroppedAsync;
    private readonly Func<
        MqttZeroQueuedMessage,
        string,
        CancellationToken,
        ValueTask> _transitionQueuedToOutboundAsync;
    private readonly Action<MqttZeroSessionState>
        _rescheduleMessageExpiry;

    internal MqttZeroSessionEgressServices(
        MqttZeroOptions options,
        TimeProvider timeProvider,
        MqttZeroFanoutDiagnostics diagnostics,
        Func<
            string,
            long,
            CancellationToken,
            ValueTask> deleteMessageAsync,
        Func<
            MqttZeroSessionState,
            MqttZeroMessageLease,
            MqttZeroMessageDropReason,
            CancellationToken,
            ValueTask> notifyDroppedAsync,
        Func<
            MqttZeroQueuedMessage,
            string,
            CancellationToken,
            ValueTask> transitionQueuedToOutboundAsync,
        Action<MqttZeroSessionState> rescheduleMessageExpiry)
    {
        Options = options ??
            throw new ArgumentNullException(nameof(options));
        TimeProvider = timeProvider ??
            throw new ArgumentNullException(nameof(timeProvider));
        Diagnostics = diagnostics ??
            throw new ArgumentNullException(nameof(diagnostics));
        _deleteMessageAsync = deleteMessageAsync ??
            throw new ArgumentNullException(nameof(deleteMessageAsync));
        _notifyDroppedAsync = notifyDroppedAsync ??
            throw new ArgumentNullException(nameof(notifyDroppedAsync));
        _transitionQueuedToOutboundAsync =
            transitionQueuedToOutboundAsync ??
            throw new ArgumentNullException(
                nameof(transitionQueuedToOutboundAsync));
        _rescheduleMessageExpiry = rescheduleMessageExpiry ??
            throw new ArgumentNullException(
                nameof(rescheduleMessageExpiry));
    }

    internal MqttZeroOptions Options { get; }

    internal TimeProvider TimeProvider { get; }

    internal MqttZeroFanoutDiagnostics Diagnostics { get; }

    internal ValueTask DeleteMessageAsync(
        string clientId,
        long messageId,
        CancellationToken cancellationToken)
    {
        return _deleteMessageAsync(
            clientId,
            messageId,
            cancellationToken);
    }

    internal ValueTask NotifyDroppedAsync(
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttZeroMessageDropReason reason,
        CancellationToken cancellationToken)
    {
        return _notifyDroppedAsync(
            session,
            lease,
            reason,
            cancellationToken);
    }

    internal ValueTask TransitionQueuedToOutboundAsync(
        MqttZeroQueuedMessage message,
        string clientId,
        CancellationToken cancellationToken)
    {
        return _transitionQueuedToOutboundAsync(
            message,
            clientId,
            cancellationToken);
    }

    internal void RescheduleMessageExpiry(
        MqttZeroSessionState session)
    {
        _rescheduleMessageExpiry(session);
    }
}
