using MqttZero.Memory;
using MqttZero.Server.Broker;
using MqttZero.Server.Observers;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Egress;

/// <summary>
/// 出站阶段使用的 Broker 服务回调。
/// </summary>
internal sealed class MqttZeroSessionEgressServices
{
    private readonly Func<string, long, CancellationToken, ValueTask> _deleteMessageAsync;
    private readonly Func<MqttZeroSessionState, MqttZeroMessageLease, MqttZeroMessageDropReason, CancellationToken, ValueTask> _notifyDroppedAsync;
    private readonly Func<MqttZeroQueuedMessage, string, CancellationToken, ValueTask> _transitionQueuedToOutboundAsync;
    private readonly Action<MqttZeroSessionState> _rescheduleMessageExpiry;

    /// <summary>
    /// 初始化 MqttZeroSessionEgressServices 的新实例。
    /// </summary>
    /// <param name="options">选项。</param>
    /// <param name="timeProvider">时间Provider。</param>
    /// <param name="diagnostics">诊断。</param>
    /// <param name="deleteMessageAsync">delete消息。</param>
    /// <param name="notifyDroppedAsync">notifyDropped。</param>
    /// <param name="transitionQueuedToOutboundAsync">transitionQueuedTo出站。</param>
    /// <param name="rescheduleMessageExpiry">reschedule消息Expiry。</param>
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

    /// <summary>
    /// 选项。
    /// </summary>
    internal MqttZeroOptions Options { get; }

    /// <summary>
    /// 时间Provider。
    /// </summary>
    internal TimeProvider TimeProvider { get; }

    /// <summary>
    /// 诊断。
    /// </summary>
    internal MqttZeroFanoutDiagnostics Diagnostics { get; }

    /// <summary>
    /// 删除消息。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="messageId">消息Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// 通知Dropped。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="lease">租约。</param>
    /// <param name="reason">原因。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// TransitionQueuedTo出站。
    /// </summary>
    /// <param name="message">消息。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
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

    /// <summary>
    /// Reschedule消息Expiry。
    /// </summary>
    /// <param name="session">会话。</param>
    internal void RescheduleMessageExpiry(
        MqttZeroSessionState session)
    {
        _rescheduleMessageExpiry(session);
    }
}
