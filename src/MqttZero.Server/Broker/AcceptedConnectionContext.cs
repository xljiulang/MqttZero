using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Security.Cryptography.X509Certificates;
using MqttZero.Server.Observers;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;

namespace MqttZero.Server.Broker;

/// <summary>
/// 连接受理过程中的可变状态。
/// </summary>
internal sealed class AcceptedConnectionContext : IDisposable
{
    public const int MaximumPendingPublishesInFlight = 16;

    private readonly MqttZeroBroker _broker;
    private readonly MqttZeroShardCommand<MqttZeroInboundBufferAction> _shardCommand = new();
    private readonly MqttZeroPendingDrain _acknowledgementDrain = new();
    private readonly MqttZeroPendingDrain _pendingPublishDrain = new();

    /// <summary>
    /// 初始化 AcceptedConnectionContext 的新实例。
    /// </summary>
    /// <param name="broker">对应的 broker。</param>
    /// <param name="transport">传输。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    public AcceptedConnectionContext(
        MqttZeroBroker broker,
        MqttZeroTransportConnection transport,
        CancellationToken cancellationToken)
    {
        _broker = broker;
        Input = transport.Input;
        Output = transport.Output;
        RemoteEndPoint = transport.RemoteEndPoint;
        IsSecure = transport.IsSecure;
        IsWebSocket = transport.IsWebSocket;
        ClientCertificate = transport.ClientCertificate;
        AllowPacketFragmentation = transport.AllowPacketFragmentation;
        AcceptedConnectionId = _broker.NextAcceptedConnectionId();
        AcceptedCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        ConnectionLifetime = CancellationTokenSource.CreateLinkedTokenSource(
            AcceptedCancellation.Token);
        ConnectTimeout = CancellationTokenSource.CreateLinkedTokenSource(
            ConnectionLifetime.Token);
        ConnectTimeout.CancelAfter(_broker.BrokerOptions.ConnectTimeout);
        CachedConnectInvocation = InvokeConnectAsync;
        CachedPacketInvocation = InvokePacketOnShardAsync;
        CachedOwnedPublishInvocation =
            InvokeOwnedPublishOnShardAsync;
        CachedPendingPublishCompleted =
            CompletePendingPublish;
        CachedPendingPublishInvocation =
            broker.ProcessQueuedFastPublishAsync;
        CachedInboundShardFailure =
            broker.HandleConnectionWorkFailure;
        CachedAcknowledgementCompleted =
            CompleteAcknowledgement;
        CachedAcknowledgementInvocation =
            broker.ProcessQueuedAcknowledgementAsync;
        CachedSynchronousAcknowledgementInvocation =
            InvokeAcknowledgementOnShardAsync;
    }

    /// <summary>
    /// Input 的值。
    /// </summary>
    public PipeReader Input { get; }

    /// <summary>
    /// Output 的值。
    /// </summary>
    public PipeWriter Output { get; }

    /// <summary>
    /// 远端EndPoint。
    /// </summary>
    public EndPoint? RemoteEndPoint { get; }

    /// <summary>
    /// 判断是否Secure。
    /// </summary>
    public bool IsSecure { get; }

    /// <summary>
    /// 判断是否WebSocket。
    /// </summary>
    public bool IsWebSocket { get; }

    /// <summary>
    /// 客户端Certificate。
    /// </summary>
    public X509Certificate2? ClientCertificate { get; }

    /// <summary>
    /// Allow报文Fragmentation。
    /// </summary>
    public bool AllowPacketFragmentation { get; }

    /// <summary>
    /// Accepted连接Id。
    /// </summary>
    public long AcceptedConnectionId { get; }

    /// <summary>
    /// Accepted取消。
    /// </summary>
    public CancellationTokenSource AcceptedCancellation { get; }

    /// <summary>
    /// 连接Lifetime。
    /// </summary>
    public CancellationTokenSource ConnectionLifetime { get; }

    /// <summary>
    /// 连接超时。
    /// </summary>
    public CancellationTokenSource ConnectTimeout { get; }

    /// <summary>
    /// 连接。
    /// </summary>
    public MqttZeroClientConnection? Connection { get; set; }

    /// <summary>
    /// 会话。
    /// </summary>
    public MqttZeroSessionState? Session { get; set; }

    /// <summary>
    /// 关闭Registration。
    /// </summary>
    public CancellationTokenRegistration CloseRegistration { get; set; }

    /// <summary>
    /// 连接Disposed。
    /// </summary>
    public bool ConnectionDisposed { get; set; }

    /// <summary>
    /// Clean断开。
    /// </summary>
    public bool CleanDisconnect { get; set; }

    /// <summary>
    /// IncompleteSince 的值。
    /// </summary>
    public DateTimeOffset? IncompleteSince { get; set; }

    /// <summary>
    /// 缓冲区。
    /// </summary>
    public ReadOnlySequence<byte> Buffer { get; set; }

    /// <summary>
    /// Examined 的值。
    /// </summary>
    public SequencePosition Examined { get; set; }

    /// <summary>
    /// 当前连接报文。
    /// </summary>
    public MqttConnectPacket? CurrentConnectPacket { get; set; }

    /// <summary>
    /// 连接客户端Id。
    /// </summary>
    public string ConnectClientId { get; set; } = string.Empty;

    /// <summary>
    /// Assigned客户端标识符。
    /// </summary>
    public bool AssignedClientIdentifier { get; set; }

    /// <summary>
    /// 会话分片Id。
    /// </summary>
    public int SessionShardId { get; set; }

    /// <summary>
    /// 当前报文。
    /// </summary>
    public MqttPacket? CurrentPacket { get; set; }

    /// <summary>
    /// 当前报文Consumed。
    /// </summary>
    public long CurrentPacketConsumed { get; set; }

    /// <summary>
    /// Cached连接Invocation。
    /// </summary>
    public Func<CancellationToken, ValueTask<ConnectionAttachment?>> CachedConnectInvocation { get; }

    /// <summary>
    /// Cached报文Invocation。
    /// </summary>
    public Func<CancellationToken, ValueTask<bool>> CachedPacketInvocation { get; }

    /// <summary>
    /// 缓存的分片内发布处理回调，复用委托避免逐条发布分配。
    /// </summary>
    public Func<CancellationToken, ValueTask<MqttZeroInboundBufferAction>> CachedOwnedPublishInvocation { get; }

    /// <summary>
    /// Cached待处理发布Completed。
    /// </summary>
    public Action<TaskCompletionSource> CachedPendingPublishCompleted { get; }

    /// <summary>
    /// 缓存的延迟发布工作项处理回调。
    /// </summary>
    public Func<MqttZeroInboundWork, CancellationToken, ValueTask> CachedPendingPublishInvocation { get; }

    /// <summary>
    /// Cached入站分片失败。
    /// </summary>
    public Action<Exception> CachedInboundShardFailure { get; }

    /// <summary>
    /// Cached确认Completed。
    /// </summary>
    public Action<TaskCompletionSource> CachedAcknowledgementCompleted { get; }

    /// <summary>
    /// 缓存的确认工作项处理回调。
    /// </summary>
    public Func<MqttZeroAcknowledgementWork, CancellationToken, ValueTask> CachedAcknowledgementInvocation { get; }

    /// <summary>
    /// 缓存的同步确认处理回调，用于不经过工作项的快速路径。
    /// </summary>
    public Func<CancellationToken, ValueTask<MqttZeroInboundBufferAction>> CachedSynchronousAcknowledgementInvocation { get; }

    /// <summary>
    /// CurrentOwnedPublishLease 的值。
    /// </summary>
    public MqttZeroMessageLease? CurrentOwnedPublishLease { get; set; }

    /// <summary>
    /// CurrentOwnedPublishPacketIdentifier 的值。
    /// </summary>
    public ushort CurrentOwnedPublishPacketIdentifier { get; set; }

    /// <summary>
    /// CurrentAcknowledgement 的值。
    /// </summary>
    public MqttZeroAcknowledgementView CurrentAcknowledgement { get; set; }

    /// <summary>
    /// PendingPublishCount 的值。
    /// </summary>
    public int PendingPublishCount
    {
        get
        {
            return _pendingPublishDrain.Count;
        }
    }

    /// <summary>
    /// Begin待处理发布。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public TaskCompletionSource BeginPendingPublish()
    {
        return _pendingPublishDrain.Begin();
    }

    /// <summary>
    /// 完成待处理发布。
    /// </summary>
    /// <param name="token">令牌。</param>
    public void CompletePendingPublish(TaskCompletionSource token)
    {
        _pendingPublishDrain.Complete(token);
    }

    /// <summary>
    /// 等待待处理Publishes。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask WaitForPendingPublishesAsync()
    {
        return _pendingPublishDrain.WaitAsync();
    }

    /// <summary>
    /// Begin确认。
    /// </summary>
    /// <returns>对应的结果。</returns>
    public TaskCompletionSource BeginAcknowledgement()
    {
        return _acknowledgementDrain.Begin();
    }

    /// <summary>
    /// 完成确认。
    /// </summary>
    /// <param name="token">令牌。</param>
    public void CompleteAcknowledgement(TaskCompletionSource token)
    {
        _acknowledgementDrain.Complete(token);
    }

    /// <summary>
    /// 等待Acknowledgements。
    /// </summary>
    /// <returns>表示异步操作的任务。</returns>
    public ValueTask WaitForAcknowledgementsAsync()
    {
        return _acknowledgementDrain.WaitAsync();
    }

    /// <summary>
    /// 调用分片Command。
    /// </summary>
    public ValueTask<MqttZeroInboundBufferAction>
        InvokeShardCommandAsync(
        MqttZeroEventLoop shard,
        Func<
            CancellationToken,
            ValueTask<MqttZeroInboundBufferAction>> operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(shard);
        ArgumentNullException.ThrowIfNull(operation);
        if (shard.IsCurrent)
        {
            return operation(cancellationToken);
        }

        _shardCommand.Prepare(operation, cancellationToken);
        return shard.InvokeAsync(
            _shardCommand,
            cancellationToken);
    }

    /// <summary>
    /// 释放。
    /// </summary>
    public void Dispose()
    {
        ConnectTimeout.Dispose();
        ConnectionLifetime.Dispose();
        AcceptedCancellation.Dispose();
    }

    /// <summary>
    /// 调用连接。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    private ValueTask<ConnectionAttachment?> InvokeConnectAsync(
        CancellationToken cancellationToken)
    {
        MqttConnectPacket connectPacket = CurrentConnectPacket ??
            throw new InvalidOperationException(
                "The CONNECT packet is not available.");
        return _broker.ConnectAsync(
            connectPacket,
            Output,
            RemoteEndPoint,
            IsSecure,
            IsWebSocket,
            ClientCertificate,
            AllowPacketFragmentation,
            ConnectClientId,
            AssignedClientIdentifier,
            SessionShardId,
            cancellationToken);
    }

    /// <summary>
    /// 调用报文On分片。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<bool> InvokePacketOnShardAsync(
        CancellationToken cancellationToken)
    {
        MqttPacket packet = CurrentPacket ??
            throw new InvalidOperationException(
                "The MQTT packet is not available.");
        MqttZeroClientConnection connection = Connection ??
            throw new InvalidOperationException(
                "The MQTT connection is not available.");
        MqttZeroSessionState session = Session ??
            throw new InvalidOperationException(
                "The MQTT session is not available.");
        connection.MarkPacketReceived(
            _broker.BrokerTimeProvider,
            CurrentPacketConsumed,
            packet is MqttPublishPacket);
        await _broker.ProcessPacketAsync(
            session,
            connection,
            packet,
            cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// 调用Owned发布On分片。
    /// </summary>
    private ValueTask<MqttZeroInboundBufferAction>
        InvokeOwnedPublishOnShardAsync(
        CancellationToken cancellationToken)
    {
        MqttZeroMessageLease lease =
            CurrentOwnedPublishLease ??
            throw new InvalidOperationException(
                "The owned PUBLISH lease is not available.");
        return _broker.ProcessOwnedPublishAsync(
            this,
            lease,
            CurrentOwnedPublishPacketIdentifier,
            cancellationToken);
    }

    /// <summary>
    /// 调用确认On分片。
    /// </summary>
    private ValueTask<MqttZeroInboundBufferAction>
        InvokeAcknowledgementOnShardAsync(
        CancellationToken cancellationToken)
    {
        MqttZeroClientConnection connection = Connection ??
            throw new InvalidOperationException(
                "The MQTT connection is not available.");
        MqttZeroSessionState session = Session ??
            throw new InvalidOperationException(
                "The MQTT session is not available.");
        return InvokeAcknowledgementCoreAsync(
            session,
            connection,
            CurrentAcknowledgement,
            cancellationToken);
    }

    /// <summary>
    /// 调用确认。
    /// </summary>
    private async ValueTask<MqttZeroInboundBufferAction>
        InvokeAcknowledgementCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroAcknowledgementView acknowledgement,
        CancellationToken cancellationToken)
    {
        await _broker.ProcessAcknowledgementCoreAsync(
            session,
            connection,
            acknowledgement,
            cancellationToken).ConfigureAwait(false);
        return MqttZeroInboundBufferAction.Handled;
    }

}
