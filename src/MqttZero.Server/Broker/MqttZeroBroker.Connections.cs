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
/// MQTT 5 Broker 主体，按连接、会话、路由、投递与生命周期拆成多个分部文件。
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// Processes one accepted connection until it disconnects.
    /// </summary>
    /// <param name="transport">The transport resources and connection metadata.</param>
    /// <param name="cancellationToken">A token that terminates the connection.</param>
    /// <returns>A task that completes when the connection closes.</returns>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="transport"/>, its input, or its output is null.
    /// </exception>
    public ValueTask AcceptConnectionAsync(
        MqttZeroTransportConnection transport,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(transport.Input);
        ArgumentNullException.ThrowIfNull(transport.Output);
        return AcceptConnectionCoreAsync(transport, cancellationToken);
    }

    /// <summary>
    /// 受理连接。
    /// </summary>
    /// <param name="transport">传输。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask AcceptConnectionCoreAsync(
        MqttZeroTransportConnection transport,
        CancellationToken cancellationToken)
    {
        using AcceptedConnectionContext context = new(
            this,
            transport,
            cancellationToken);
        if (!TryAcceptConnection(context))
        {
            await context.Output.CompleteAsync().ConfigureAwait(false);
            return;
        }

        try
        {
            await RunConnectionAsync(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (InvalidOperationException) when (context.Session == null)
        {
        }
        finally
        {
            await CleanupAcceptedConnectionAsync(context).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 尝试Accept连接。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private bool TryAcceptConnection(AcceptedConnectionContext context)
    {
        lock (_sessionGate)
        {
            if (!_options.AcceptNewConnections ||
                State != MqttZeroBrokerState.Running ||
                Volatile.Read(ref _activeConnectionCount) >=
                    _options.MaxActiveConnections)
            {
                return false;
            }

            Interlocked.Increment(ref _activeConnectionCount);
            _acceptedConnections[context.AcceptedConnectionId] =
                context.AcceptedCancellation;
            return true;
        }
    }

    /// <summary>
    /// 运行连接。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RunConnectionAsync(AcceptedConnectionContext context)
    {
        while (!context.ConnectionLifetime.IsCancellationRequested)
        {
            ReadResult readResult = await ReadConnectionBufferAsync(context)
                .ConfigureAwait(false);
            context.Buffer = readResult.Buffer;
            context.Examined = readResult.Buffer.End;

            if (MqttPacketCodec.TryGetPacketLength(
                    context.Buffer,
                    out long packetLength,
                    out _) &&
                packetLength > _options.MaxPacketSize)
            {
                throw new InvalidOperationException(
                    "The inbound MQTT packet exceeds the broker maximum.");
            }

            if (!await ProcessConnectionBufferAsync(context).ConfigureAwait(false))
            {
                return;
            }

            UpdateIncompletePacket(context, !context.Buffer.IsEmpty);

            if (readResult.IsCompleted)
            {
                context.Input.AdvanceTo(context.Buffer.Start);
                return;
            }

            context.Input.AdvanceTo(context.Buffer.Start, context.Examined);
        }
    }

    /// <summary>
    /// 读取连接缓冲区。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<ReadResult> ReadConnectionBufferAsync(
        AcceptedConnectionContext context)
    {
        CancellationToken readToken = context.Session == null
            ? context.ConnectTimeout.Token
            : context.ConnectionLifetime.Token;
        using CancellationTokenSource? incompleteTimeout =
            context.Session != null && context.IncompleteSince.HasValue
                ? CancellationTokenSource.CreateLinkedTokenSource(readToken)
                : null;
        if (incompleteTimeout != null &&
            context.IncompleteSince is { } incompleteStart)
        {
            TimeSpan remaining =
                _options.IncompletePacketTimeout -
                (_timeProvider.GetUtcNow() - incompleteStart);
            if (remaining <= TimeSpan.Zero)
            {
                throw new OperationCanceledException(
                    "The incomplete MQTT packet timed out.");
            }

            incompleteTimeout.CancelAfter(remaining);
            readToken = incompleteTimeout.Token;
        }

        return await context.Input.ReadAsync(readToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 处理连接缓冲区。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<bool> ProcessConnectionBufferAsync(
        AcceptedConnectionContext context)
    {
        while (!context.Buffer.IsEmpty)
        {
            MqttZeroClientConnection? connection = context.Connection;
            MqttZeroSessionState? session = context.Session;
            if (session != null &&
                connection != null &&
                (context.Buffer.Slice(0, 1).FirstSpan[0] >> 4) == 0x0F)
            {
                await connection.CloseAsync(
                    MqttReasonCode.ProtocolError,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
                return false;
            }

            MqttZeroInboundBufferAction publishAction =
                await ProcessFastPublishAsync(context).ConfigureAwait(false);
            if (publishAction == MqttZeroInboundBufferAction.Stop)
            {
                return false;
            }

            if (publishAction == MqttZeroInboundBufferAction.Handled)
            {
                continue;
            }

            MqttZeroInboundBufferAction acknowledgementAction =
                await ProcessAcknowledgementAsync(context).ConfigureAwait(false);
            if (acknowledgementAction == MqttZeroInboundBufferAction.Stop)
            {
                return false;
            }

            if (acknowledgementAction == MqttZeroInboundBufferAction.Handled)
            {
                continue;
            }

            if (!MqttPacketCodec.TryDecode(
                    context.Buffer,
                    out MqttPacket? packet,
                    out long consumed) ||
                packet == null)
            {
                return true;
            }

            if (context.Session == null)
            {
                if (!await ProcessConnectPacketAsync(context, packet, consumed)
                    .ConfigureAwait(false))
                {
                    return false;
                }

                continue;
            }

            if (!await ProcessDecodedPacketAsync(context, packet, consumed)
                .ConfigureAwait(false))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// 处理Fast发布。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<MqttZeroInboundBufferAction> ProcessFastPublishAsync(
        AcceptedConnectionContext context)
    {
        if (!context.Buffer.IsEmpty)
        {
            byte fixedHeader =
                context.Buffer.Slice(0, 1).FirstSpan[0];
            bool isQosZeroPublish =
                (fixedHeader >> 4) == 0x03 &&
                (fixedHeader & 0x06) == 0;
            if (isQosZeroPublish &&
                context.PendingPublishCount >=
                    AcceptedConnectionContext.
                        MaximumPendingPublishesInFlight)
            {
                await context.WaitForPendingPublishesAsync()
                    .ConfigureAwait(false);
            }
        }

        if (context.Session == null ||
            context.Connection == null ||
            !MqttPublishPacketCodec.TryDecode(
                context.Buffer,
                out MqttZeroPublishView publishView,
                out long publishConsumed))
        {
            return MqttZeroInboundBufferAction.NotHandled;
        }

        MqttZeroClientConnection connection = context.Connection;
        ushort publishPacketIdentifier =
            publishView.PacketIdentifier;
        connection.MarkPacketReceived(
            _timeProvider,
            publishConsumed,
            true);
        try
        {
            if (!TryCreateMessageLease(
                    connection,
                    publishView,
                    out MqttZeroMessageLease? candidate,
                    out MqttReasonCode reasonCode))
            {
                bool stop = await RejectInvalidPublishAsync(
                    connection,
                    publishView.QualityOfServiceLevel,
                    publishPacketIdentifier,
                    reasonCode,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
                context.Buffer = context.Buffer.Slice(publishConsumed);
                return stop
                    ? MqttZeroInboundBufferAction.Stop
                    : MqttZeroInboundBufferAction.Handled;
            }

            context.CurrentOwnedPublishLease = candidate;
            context.CurrentOwnedPublishPacketIdentifier =
                publishPacketIdentifier;
            if (candidate.QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtMostOnce)
            {
                await OwnInboundLeaseAsync(
                    candidate,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
                try
                {
                    if (CanRouteQosZeroDirectly(context, candidate))
                    {
                        _fanoutDiagnostics.RecordQosZeroRoute(direct: true);
                        await RoutePublishLeaseFastAsync(
                            context.Session!,
                            connection,
                            candidate,
                            suppressPublisherAck: false,
                            cancellationToken:
                                context.ConnectionLifetime.Token,
                            scheduleQosZeroWithoutWaiting: true)
                            .ConfigureAwait(false);
                        context.Buffer =
                            context.Buffer.Slice(publishConsumed);
                        return MqttZeroInboundBufferAction.Handled;
                    }

                    _fanoutDiagnostics.RecordQosZeroRoute(direct: false);
                    MqttZeroEventLoop shard =
                        TryPrepareFastPublish(
                        context,
                        context.Session,
                        connection,
                        candidate,
                        publishPacketIdentifier,
                        out MqttZeroInboundShardWork shardWork);
                    if (!shard.TryEnqueue(shardWork))
                    {
                        try
                        {
                            await shard.EnqueueAsync(
                                shardWork,
                                context.ConnectionLifetime.Token)
                                .ConfigureAwait(false);
                        }
                        catch
                        {
                            shardWork.Release();
                            throw;
                        }
                    }
                }
                finally
                {
                    candidate.Release();
                    context.CurrentOwnedPublishLease = null;
                }

                context.Buffer = context.Buffer.Slice(publishConsumed);
                return MqttZeroInboundBufferAction.Handled;
            }

            if (candidate.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce &&
                CanDispatchInbound)
            {
                await OwnInboundLeaseAsync(
                    candidate,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
                MqttZeroInboundBufferAction qosOneAction =
                    await ProcessQosOnePublishDirectAsync(
                        context.Session!,
                        connection,
                        candidate,
                        context.ConnectionLifetime.Token)
                        .ConfigureAwait(false);
                context.CurrentOwnedPublishLease = null;
                if (qosOneAction == MqttZeroInboundBufferAction.Stop)
                {
                    return MqttZeroInboundBufferAction.Stop;
                }

                context.Buffer = context.Buffer.Slice(publishConsumed);
                return MqttZeroInboundBufferAction.Handled;
            }

            MqttZeroInboundBufferAction action;
            try
            {
                action = await context.InvokeShardCommandAsync(
                    ShardSet[context.Session.ShardId],
                    context.CachedOwnedPublishInvocation,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
            }
            finally
            {
                context.CurrentOwnedPublishLease = null;
            }
            if (action == MqttZeroInboundBufferAction.Stop)
            {
                return MqttZeroInboundBufferAction.Stop;
            }

            context.Buffer = context.Buffer.Slice(publishConsumed);
            return MqttZeroInboundBufferAction.Handled;
        }
        catch (MqttZeroBufferPoolExhaustedException)
        {
            await connection.CloseAsync(
                MqttReasonCode.QuotaExceeded,
                context.ConnectionLifetime.Token).ConfigureAwait(false);
            return MqttZeroInboundBufferAction.Stop;
        }
    }

    /// <summary>
    /// 拒绝Invalid发布。
    /// </summary>
    /// <param name="connection">连接。</param>
    /// <param name="qualityOfServiceLevel">服务质量ServiceLevel。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    private static async ValueTask<bool> RejectInvalidPublishAsync(
        MqttZeroClientConnection connection,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        if (qualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtLeastOnce &&
            reasonCode == MqttReasonCode.PayloadFormatInvalid)
        {
            await connection.SendPubAckAsync(
                packetIdentifier,
                reasonCode,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (qualityOfServiceLevel ==
                MqttQualityOfServiceLevel.ExactlyOnce &&
            reasonCode == MqttReasonCode.PayloadFormatInvalid)
        {
            await connection.SendPubRecAsync(
                packetIdentifier,
                reasonCode,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        await connection.CloseAsync(
            reasonCode,
            cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// 处理Owned发布。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="lease">租约。</param>
    /// <param name="inboundPacketIdentifier">入站报文标识符。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<MqttZeroInboundBufferAction> ProcessOwnedPublishAsync(
        AcceptedConnectionContext context,
        MqttZeroMessageLease lease,
        ushort inboundPacketIdentifier,
        CancellationToken cancellationToken)
    {
        await OwnInboundLeaseAsync(lease, cancellationToken).ConfigureAwait(false);
        return await ProcessOwnedPublishCoreAsync(
            context.Session!,
            context.Connection!,
            lease,
            inboundPacketIdentifier,
            releaseLease: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 处理QosOne发布Direct。
    /// </summary>
    private async ValueTask<MqttZeroInboundBufferAction>
        ProcessQosOnePublishDirectAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        bool transferred = false;
        try
        {
            PublishDispatchResult result =
                await DispatchQosOnePublishAsync(
                    session,
                    connection,
                    lease,
                    cancellationToken).ConfigureAwait(false);
            transferred = result == PublishDispatchResult.Transferred;
            return result == PublishDispatchResult.Stop
                ? MqttZeroInboundBufferAction.Stop
                : MqttZeroInboundBufferAction.Handled;
        }
        finally
        {
            if (!transferred)
            {
                lease.Release();
            }
        }
    }

    /// <summary>
    /// 处理Owned发布。
    /// </summary>
    private async ValueTask<MqttZeroInboundBufferAction>
        ProcessOwnedPublishCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        ushort inboundPacketIdentifier,
        bool releaseLease,
        CancellationToken cancellationToken)
    {
        bool transferred = false;
        try
        {
            if (lease.QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.ExactlyOnce)
            {
                transferred = await AcceptInboundQos2Async(
                    session,
                    connection,
                    lease,
                    inboundPacketIdentifier,
                    releaseSourceLease: releaseLease,
                    cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
            else if (lease.QualityOfServiceLevel ==
                    MqttQualityOfServiceLevel.AtLeastOnce &&
                CanDispatchInbound)
            {
                PublishDispatchResult result = await DispatchQosOnePublishAsync(
                    session,
                    connection,
                    lease,
                    cancellationToken).ConfigureAwait(false);
                transferred = result == PublishDispatchResult.Transferred;
                if (result == PublishDispatchResult.Stop)
                {
                    return MqttZeroInboundBufferAction.Stop;
                }
            }
            else
            {
                await RoutePublishLeaseAsync(
                    session,
                    connection,
                    lease,
                    cancellationToken).ConfigureAwait(false);
            }

            return MqttZeroInboundBufferAction.Handled;
        }
        finally
        {
            if (releaseLease && !transferred)
            {
                lease.Release();
            }
        }
    }

    /// <summary>
    /// 尝试PrepareFast发布。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="lease">租约。</param>
    /// <param name="packetIdentifier">报文标识符。</param>
    /// <param name="shardWork">分片工作项。</param>
    /// <returns>对应的结果。</returns>
    private MqttZeroEventLoop TryPrepareFastPublish(
        AcceptedConnectionContext context,
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        ushort packetIdentifier,
        out MqttZeroInboundShardWork shardWork)
    {
        TaskCompletionSource completion = context.BeginPendingPublish();
        MqttZeroInboundWork work = MqttZeroInboundWork.Rent(
            lease,
            session,
            connection,
            packetIdentifier,
            context.CachedPendingPublishCompleted,
            completion);
        try
        {
            shardWork = MqttZeroInboundShardWork.Rent(
                work,
                context.CachedPendingPublishInvocation,
                context.CachedInboundShardFailure);
        }
        catch
        {
            work.Release();
            throw;
        }

        return ShardSet[session.ShardId];
    }

    /// <summary>
    /// 判断是否可以RouteQosZeroDirectly。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="lease">租约。</param>
    /// <returns>成功时为 <see langword="true"/>；否则为 <see langword="false"/>。</returns>
    private bool CanRouteQosZeroDirectly(
        AcceptedConnectionContext context,
        MqttZeroMessageLease lease)
    {
        bool anyStoreEnabled =
            _sessionStore.IsEnabled ||
            _messageStore.IsEnabled ||
            _retainedMessageStore.IsEnabled;
        bool anyExtensionRegistered =
            _publishInterceptors.Length != 0 ||
            _enqueueInterceptors.Length != 0 ||
            _deliveryObservers.Length != 0;
        return MqttZeroQosZeroDirectRouting.CanRoute(
            context.PendingPublishCount,
            lease.RetainFlag,
            anyStoreEnabled,
            anyExtensionRegistered);
    }

    /// <summary>
    /// 处理QueuedFast发布。
    /// </summary>
    /// <param name="work">工作项。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ProcessQueuedFastPublishAsync(
        MqttZeroInboundWork work,
        CancellationToken cancellationToken)
    {
        MqttZeroClientConnection? connection = work.SenderConnection;
        try
        {
            MqttZeroInboundBufferAction action =
                await ProcessOwnedPublishCoreAsync(
                    work.Sender,
                    connection ??
                        throw new InvalidOperationException(
                            "The fast publisher connection is unavailable."),
                    work.Lease,
                    checked((ushort)work.MessageId),
                    releaseLease: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            if (action == MqttZeroInboundBufferAction.Stop)
            {
                await connection.CloseAsync(
                    MqttReasonCode.UnspecifiedError,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            if (connection != null &&
                !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await connection.CloseAsync(
                        MqttReasonCode.UnspecifiedError,
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    connection.Abort();
                }
            }

            throw;
        }
    }

    /// <summary>
    /// 入队确认。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="acknowledgement">确认。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask EnqueueAcknowledgementAsync(
        AcceptedConnectionContext context,
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroAcknowledgementView acknowledgement,
        CancellationToken cancellationToken)
    {
        TaskCompletionSource completion = context.BeginAcknowledgement();
        MqttZeroAcknowledgementWork? work = null;
        MqttZeroAcknowledgementShardWork? shardWork = null;
        try
        {
            work = MqttZeroAcknowledgementWork.Rent(
                session,
                connection,
                acknowledgement,
                context.CachedAcknowledgementCompleted,
                completion);
            if (_fanoutDiagnostics.Enabled)
            {
                work.EnqueuedTimestampTicks =
                    System.Diagnostics.Stopwatch.GetTimestamp();
            }

            shardWork = MqttZeroAcknowledgementShardWork.Rent(
                work,
                context.CachedAcknowledgementInvocation,
                context.CachedInboundShardFailure);
            MqttZeroEventLoop shard = ShardSet[session.ShardId];
            if (!shard.TryEnqueue(shardWork))
            {
                await shard.EnqueueAsync(
                    shardWork,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            if (shardWork != null)
            {
                shardWork.Release();
            }
            else if (work != null)
            {
                work.Release();
            }

            throw;
        }
    }

    /// <summary>
    /// 处理Queued确认。
    /// </summary>
    /// <param name="work">工作项。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ProcessQueuedAcknowledgementAsync(
        MqttZeroAcknowledgementWork work,
        CancellationToken cancellationToken)
    {
        if (work.EnqueuedTimestampTicks != 0)
        {
            _fanoutDiagnostics.RecordAcknowledgementQueue(
                work.EnqueuedTimestampTicks);
        }

        try
        {
            await ProcessAcknowledgementCoreAsync(
                work.Session,
                work.Connection,
                work.Acknowledgement,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await work.Connection.CloseAsync(
                        MqttReasonCode.UnspecifiedError,
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    work.Connection.Abort();
                }
            }

            throw;
        }
    }

    /// <summary>
    /// 分发QosOne发布。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="lease">租约。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<PublishDispatchResult> DispatchQosOnePublishAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        MqttZeroPreparedPublish prepared =
            await PreparePublishForDispatchAsync(
                session,
                connection,
                lease,
                cancellationToken).ConfigureAwait(false);
        if (prepared.Result == MqttZeroInterceptionResult.CloseConnection)
        {
            await connection.CloseAsync(
                prepared.ReasonCode == MqttReasonCode.Success
                    ? MqttReasonCode.NotAuthorized
                    : prepared.ReasonCode,
                cancellationToken).ConfigureAwait(false);
            return PublishDispatchResult.Stop;
        }

        if (prepared.Result != MqttZeroInterceptionResult.Continue ||
            prepared.Lease == null)
        {
            await connection.SendPubAckAsync(
                lease.PacketIdentifier,
                prepared.ReasonCode,
                cancellationToken).ConfigureAwait(false);
            return PublishDispatchResult.NotTransferred;
        }

        long messageId = _messageStore.IsEnabled
            ? GetNextMessageId()
            : 0;
        try
        {
            await SaveInboundDispatchAsync(
                prepared.Lease,
                session,
                messageId,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            if (!prepared.UsesSourceLease)
            {
                prepared.Lease.Release();
            }

            throw;
        }

        try
        {
            await EnqueueInboundAsync(
                prepared.Lease,
                session,
                connection,
                cancellationToken,
                messageId).ConfigureAwait(false);
        }
        catch
        {
            await DeleteInboundDispatchAsync(
                session.ClientId,
                messageId,
                CancellationToken.None).ConfigureAwait(false);
            if (!prepared.UsesSourceLease)
            {
                prepared.Lease.Release();
            }

            throw;
        }

        if (!prepared.UsesSourceLease)
        {
            lease.Release();
        }

        prepared.Lease.Release();
        await connection.SendPubAckAsync(
            lease.PacketIdentifier,
            MqttReasonCode.Success,
            cancellationToken).ConfigureAwait(false);
        return PublishDispatchResult.Transferred;
    }

    /// <summary>
    /// 处理确认。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<MqttZeroInboundBufferAction> ProcessAcknowledgementAsync(
        AcceptedConnectionContext context)
    {
        if (context.Session == null ||
            context.Connection == null ||
            context.Buffer.IsEmpty ||
            !MqttPacketCodec.IsAcknowledgementPacketType(
                (MqttPacketType)(context.Buffer.Slice(0, 1).FirstSpan[0] >> 4)))
        {
            return MqttZeroInboundBufferAction.NotHandled;
        }

        MqttZeroClientConnection connection = context.Connection;
        MqttZeroSessionState session = context.Session;
        if (MqttPacketCodec.TryDecodeAcknowledgement(
                context.Buffer,
                out MqttZeroAcknowledgementView acknowledgement,
                out long acknowledgementConsumed))
        {
            connection.MarkPacketReceived(
                _timeProvider,
                acknowledgementConsumed,
                false);
            if (acknowledgement.PacketType == MqttPacketType.PubAck)
            {
                await EnqueueAcknowledgementAsync(
                    context,
                    session,
                    connection,
                    acknowledgement,
                    context.ConnectionLifetime.Token).ConfigureAwait(false);
            }
            else
            {
                context.CurrentAcknowledgement = acknowledgement;
                try
                {
                    await context.InvokeShardCommandAsync(
                        ShardSet[session.ShardId],
                        context.CachedSynchronousAcknowledgementInvocation,
                        context.ConnectionLifetime.Token).ConfigureAwait(false);
                }
                finally
                {
                    context.CurrentAcknowledgement = default;
                }
            }

            context.Buffer = context.Buffer.Slice(acknowledgementConsumed);
            return MqttZeroInboundBufferAction.Handled;
        }

        if (MqttPacketCodec.TryGetPacketLength(
                context.Buffer,
                out _,
                out bool acknowledgementComplete) &&
            acknowledgementComplete)
        {
            await connection.CloseAsync(
                MqttReasonCode.ProtocolError,
                context.ConnectionLifetime.Token).ConfigureAwait(false);
            return MqttZeroInboundBufferAction.Stop;
        }

        return MqttZeroInboundBufferAction.NotHandled;
    }

    /// <summary>
    /// 处理确认。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="acknowledgement">确认。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ProcessAcknowledgementCoreAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroAcknowledgementView acknowledgement,
        CancellationToken cancellationToken)
    {
        MqttZeroShardOwnership.EnsureCurrent(
            session.ShardId,
            nameof(ProcessAcknowledgementCoreAsync),
            _fanoutDiagnostics);
        _fanoutDiagnostics.RecordAcknowledgement();
        switch (acknowledgement.PacketType)
        {
            case MqttPacketType.PubAck:
                await ProcessOutboundPubAckAsync(
                    session,
                    connection,
                    acknowledgement.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPacketType.PubRec:
                await ProcessOutboundPubRecAsync(
                    session,
                    connection,
                    acknowledgement.PacketIdentifier,
                    acknowledgement.ReasonCode,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPacketType.PubRel:
                await ProcessInboundPubRelAsync(
                    session,
                    connection,
                    acknowledgement.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
            case MqttPacketType.PubComp:
                await ProcessOutboundPubCompAsync(
                    session,
                    connection,
                    acknowledgement.PacketIdentifier,
                    cancellationToken).ConfigureAwait(false);
                break;
        }
    }

    /// <summary>
    /// 处理连接报文。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="packet">报文。</param>
    /// <param name="consumed">对应的 consumed。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<bool> ProcessConnectPacketAsync(
        AcceptedConnectionContext context,
        MqttPacket packet,
        long consumed)
    {
        if (packet is not MqttConnectPacket connectPacket)
        {
            return false;
        }

        context.CurrentConnectPacket = connectPacket;
        context.AssignedClientIdentifier = connectPacket.ClientId.IsEmpty;
        context.ConnectClientId = context.AssignedClientIdentifier
            ? $"mqttzero-{Guid.NewGuid():N}"
            : Encoding.UTF8.GetString(connectPacket.ClientId.ToArray());
        context.SessionShardId = SessionShardMap.GetShardId(
            context.ConnectClientId);
        ConnectionAttachment? attachment = await ShardSet[context.SessionShardId]
            .InvokeAsync(
                context.CachedConnectInvocation,
                context.ConnectionLifetime.Token)
            .ConfigureAwait(false);
        context.CurrentConnectPacket = null;
        if (attachment == null)
        {
            return false;
        }

        context.Session = attachment.Session;
        context.Connection = attachment.Connection;
        context.CloseRegistration = attachment.Connection.ClosedToken.Register(
            CancelAcceptedConnection,
            context.ConnectionLifetime);
        await ShardSet[context.SessionShardId].InvokeAsync(
            _ =>
            {
                StartEgress(
                    attachment.Session,
                    attachment.Connection,
                    context.ConnectionLifetime.Token);
                return ValueTask.FromResult(true);
            },
            context.ConnectionLifetime.Token).ConfigureAwait(false);
        context.Buffer = context.Buffer.Slice(consumed);
        return true;
    }

    /// <summary>
    /// 处理Decoded报文。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="packet">报文。</param>
    /// <param name="consumed">对应的 consumed。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<bool> ProcessDecodedPacketAsync(
        AcceptedConnectionContext context,
        MqttPacket packet,
        long consumed)
    {
        if (context.Connection == null || context.Session == null)
        {
            return false;
        }

        context.CurrentPacket = packet;
        context.CurrentPacketConsumed = consumed;
        await ShardSet[context.Connection.ShardId]
            .InvokeAsync(
                context.CachedPacketInvocation,
                context.ConnectionLifetime.Token)
            .ConfigureAwait(false);
        context.CurrentPacket = null;
        context.Buffer = context.Buffer.Slice(consumed);

        if (packet is MqttDisconnectPacket)
        {
            context.CleanDisconnect = true;
            context.Connection.DisconnectType =
                MqttZeroDisconnectType.ClientRequest;
            return false;
        }

        return true;
    }

    /// <summary>
    /// 更新Incomplete报文。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <param name="incompletePacket">incomplete报文。</param>
    private void UpdateIncompletePacket(
        AcceptedConnectionContext context,
        bool incompletePacket)
    {
        if (context.Connection != null)
        {
            context.Connection.IsReadingIncompletePacket = incompletePacket;
        }

        if (!incompletePacket)
        {
            context.IncompleteSince = null;
            return;
        }

        context.IncompleteSince ??= _timeProvider.GetUtcNow();
    }

    /// <summary>
    /// CleanupAccepted连接。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask CleanupAcceptedConnectionAsync(
        AcceptedConnectionContext context)
    {
        context.CloseRegistration.Dispose();
        try
        {
            await context.WaitForPendingPublishesAsync()
                .ConfigureAwait(false);
            await context.WaitForAcknowledgementsAsync()
                .ConfigureAwait(false);
            await DetachAcceptedConnectionAsync(context).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogConnectionCleanupFailure(_logger, exception);
        }
        finally
        {
            await ReleaseAcceptedConnectionAsync(context).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 拆离Accepted连接。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask DetachAcceptedConnectionAsync(
        AcceptedConnectionContext context)
    {
        try
        {
            if (context.Connection != null)
            {
                ReleaseConnectedClientCount(context.Connection);
            }

            if (context.Session == null || context.Connection == null)
            {
                return;
            }

            context.Connection.IsReadingIncompletePacket = false;
            await DetachConnectionAsync(
                context.Session,
                context.Connection,
                context.CleanDisconnect).ConfigureAwait(false);
            await NotifyDisconnectedAsync(context.Connection).ConfigureAwait(false);
        }
        finally
        {
            if (context.Connection != null && !context.ConnectionDisposed)
            {
                context.ConnectionDisposed = true;
                await context.Connection.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// 通知Disconnected。
    /// </summary>
    /// <param name="connection">连接。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask NotifyDisconnectedAsync(
        MqttZeroClientConnection connection)
    {
        MqttZeroClientDisconnectedContext disconnectedContext = new()
        {
            Client = connection.ClientInfo,
            DisconnectType = connection.DisconnectType,
            ReasonCode = connection.CloseReasonCode
        };
        foreach (IMqttZeroConnectionObserver observer in _connectionObservers)
        {
            try
            {
                await observer.OnDisconnectedAsync(
                    disconnectedContext,
                    CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
            }
        }
    }

    /// <summary>
    /// 释放Accepted连接。
    /// </summary>
    /// <param name="context">上下文。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ReleaseAcceptedConnectionAsync(
        AcceptedConnectionContext context)
    {
        _acceptedConnections.TryRemove(context.AcceptedConnectionId, out _);
        int remaining = Interlocked.Decrement(ref _activeConnectionCount);
        if (remaining == 0)
        {
            _connectionsDrained.TrySetResult();
        }

        try
        {
            await context.Input.CompleteAsync().ConfigureAwait(false);
        }
        finally
        {
            if (context.Connection == null)
            {
                await context.Output.CompleteAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// 发布分发的处理结果。
    /// </summary>
    private enum PublishDispatchResult
    {
        NotTransferred,
        Transferred,
        Stop
    }

    /// <summary>
    /// 连接受理过程中的可变状态。
    /// </summary>
    private sealed class AcceptedConnectionContext : IDisposable
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
            AcceptedConnectionId = Interlocked.Increment(
                ref _broker._nextAcceptedConnectionId);
            AcceptedCancellation =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            ConnectionLifetime = CancellationTokenSource.CreateLinkedTokenSource(
                AcceptedCancellation.Token);
            ConnectTimeout = CancellationTokenSource.CreateLinkedTokenSource(
                ConnectionLifetime.Token);
            ConnectTimeout.CancelAfter(_broker._options.ConnectTimeout);
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
                _broker._timeProvider,
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

    /// <summary>
    /// 一次 CONNECT 处理的事务状态，失败时用于回滚。
    /// </summary>
    private sealed class ConnectTransaction
    {
        /// <summary>
        /// 初始化 ConnectTransaction 的新实例。
        /// </summary>
        /// <param name="broker">对应的 broker。</param>
        /// <param name="connectPacket">连接报文。</param>
        /// <param name="output">对应的 output。</param>
        /// <param name="remoteEndPoint">远端EndPoint。</param>
        /// <param name="isSecure">对应的 isSecure。</param>
        /// <param name="isWebSocket">对应的 isWebSocket。</param>
        /// <param name="clientCertificate">客户端Certificate。</param>
        /// <param name="allowPacketFragmentation">allow报文Fragmentation。</param>
        /// <param name="clientId">客户端Id。</param>
        /// <param name="assignedClientIdentifier">assigned客户端标识符。</param>
        /// <param name="sessionShardId">会话分片Id。</param>
        public ConnectTransaction(
            MqttZeroBroker broker,
            MqttConnectPacket connectPacket,
            PipeWriter output,
            EndPoint? remoteEndPoint,
            bool isSecure,
            bool isWebSocket,
            X509Certificate2? clientCertificate,
            bool allowPacketFragmentation,
            string clientId,
            bool assignedClientIdentifier,
            int sessionShardId)
        {
            ConnectPacket = connectPacket;
            ClientId = clientId;
            AssignedClientIdentifier = assignedClientIdentifier;
            EffectiveExpiry = broker.ClampSessionExpiry(
                connectPacket.SessionExpiryInterval ?? 0);
            Now = broker._timeProvider.GetUtcNow();
            NewConnection = new MqttZeroClientConnection(
                output,
                remoteEndPoint,
                isSecure,
                isWebSocket,
                connectPacket.KeepAliveSeconds,
                connectPacket.ReceiveMaximum,
                connectPacket.MaximumPacketSize,
                broker._timeProvider,
                broker._bufferPool,
                allowPacketFragmentation,
                connectPacket.RequestProblemInformation,
                connectPacket.RequestResponseInformation,
                sessionShardId);
            ClientInfo = new MqttZeroClientConnectionInfo
            {
                ClientId = clientId,
                UserName = connectPacket.HasUserName
                    ? Encoding.UTF8.GetString(connectPacket.UserName.ToArray())
                    : null,
                RemoteEndPoint = remoteEndPoint,
                IsSecure = isSecure,
                IsWebSocket = isWebSocket,
                ProtocolVersion = connectPacket.ProtocolVersion,
                ClientCertificate = clientCertificate
            };
            NewConnection.ClientInfo = ClientInfo;
        }

        /// <summary>
        /// 连接报文。
        /// </summary>
        public MqttConnectPacket ConnectPacket { get; }

        /// <summary>
        /// 客户端Id。
        /// </summary>
        public string ClientId { get; }

        /// <summary>
        /// Assigned客户端标识符。
        /// </summary>
        public bool AssignedClientIdentifier { get; }

        /// <summary>
        /// EffectiveExpiry 的值。
        /// </summary>
        public uint EffectiveExpiry { get; }

        /// <summary>
        /// 当前时间。
        /// </summary>
        public DateTimeOffset Now { get; }

        /// <summary>
        /// New连接。
        /// </summary>
        public MqttZeroClientConnection NewConnection { get; }

        /// <summary>
        /// 客户端Info。
        /// </summary>
        public MqttZeroClientConnectionInfo ClientInfo { get; }

        /// <summary>
        /// 会话。
        /// </summary>
        public MqttZeroSessionState Session { get; set; } = null!;

        /// <summary>
        /// 会话Present。
        /// </summary>
        public bool SessionPresent { get; set; }

        /// <summary>
        /// Replacing会话。
        /// </summary>
        public bool ReplacingSession { get; set; }

        /// <summary>
        /// New会话Persisted。
        /// </summary>
        public bool NewSessionPersisted { get; set; }

        /// <summary>
        /// ExistingPersistenceDeleted 的值。
        /// </summary>
        public bool ExistingPersistenceDeleted { get; set; }

        /// <summary>
        /// Replaced会话。
        /// </summary>
        public MqttZeroSessionState? ReplacedSession { get; set; }

        /// <summary>
        /// 前一个CleanStart。
        /// </summary>
        public bool PreviousCleanStart { get; set; }

        /// <summary>
        /// 前一个Expiry间隔。
        /// </summary>
        public uint PreviousExpiryInterval { get; set; }

        /// <summary>
        /// 前一个DisconnectedAt。
        /// </summary>
        public DateTimeOffset? PreviousDisconnectedAt { get; set; }

        /// <summary>
        /// 前一个遗嘱消息。
        /// </summary>
        public MqttZeroWillMessage? PreviousWillMessage { get; set; }

        /// <summary>
        /// 前一个遗嘱DueAt。
        /// </summary>
        public DateTimeOffset? PreviousWillDueAt { get; set; }

        /// <summary>
        /// 前一个Pump会话。
        /// </summary>
        public MqttZeroSessionState? PreviousPumpSession { get; set; }

        /// <summary>
        /// 前一个Pump。
        /// </summary>
        public MqttZeroSessionEgress? PreviousPump { get; set; }

        /// <summary>
        /// 前一个PumpStopped。
        /// </summary>
        public bool PreviousPumpStopped { get; set; }

        /// <summary>
        /// Old连接。
        /// </summary>
        public MqttZeroClientConnection? OldConnection { get; set; }

        /// <summary>
        /// Takeover遗嘱。
        /// </summary>
        public MqttZeroWillMessage? TakeoverWill { get; set; }
    }

    /// <summary>
    /// 连接。
    /// </summary>
    /// <param name="connectPacket">连接报文。</param>
    /// <param name="output">对应的 output。</param>
    /// <param name="remoteEndPoint">远端EndPoint。</param>
    /// <param name="isSecure">对应的 isSecure。</param>
    /// <param name="isWebSocket">对应的 isWebSocket。</param>
    /// <param name="clientCertificate">客户端Certificate。</param>
    /// <param name="allowPacketFragmentation">allow报文Fragmentation。</param>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="assignedClientIdentifier">assigned客户端标识符。</param>
    /// <param name="sessionShardId">会话分片Id。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<ConnectionAttachment?> ConnectAsync(
        MqttConnectPacket connectPacket,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        X509Certificate2? clientCertificate,
        bool allowPacketFragmentation,
        string clientId,
        bool assignedClientIdentifier,
        int sessionShardId,
        CancellationToken cancellationToken)
    {
        MqttZeroShardOwnership.EnsureCurrent(
            sessionShardId,
            nameof(ConnectAsync),
            _fanoutDiagnostics);
        ConnectTransaction transaction = new(
            this,
            connectPacket,
            output,
            remoteEndPoint,
            isSecure,
            isWebSocket,
            clientCertificate,
            allowPacketFragmentation,
            clientId,
            assignedClientIdentifier,
            sessionShardId);
        MqttZeroClientConnection newConnection = transaction.NewConnection;
        if (!await PrepareConnectAsync(transaction).ConfigureAwait(false))
        {
            return null;
        }

        try
        {
            try
            {
                AttachSession(transaction);
            }
            catch
            {
                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            try
            {
                await PersistSessionAsync(transaction).ConfigureAwait(false);
            }
            catch
            {
                await RollbackTransactionAsync(
                    transaction,
                    registerDeadlines: true,
                    removeDeadlines: false,
                    persistRestoredSession: false).ConfigureAwait(false);

                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }

            try
            {
                await ActivateConnectionAsync(transaction).ConfigureAwait(false);
                return new ConnectionAttachment(
                    transaction.Session,
                    transaction.NewConnection);
            }
            catch
            {
                ReleaseConnectedClientCount(newConnection);
                await RollbackTransactionAsync(
                    transaction,
                    registerDeadlines: false,
                    removeDeadlines: true,
                    persistRestoredSession: true).ConfigureAwait(false);

                await newConnection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            await newConnection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Prepare连接。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <returns>异步操作的结果。</returns>
    private async ValueTask<bool> PrepareConnectAsync(
        ConnectTransaction transaction)
    {
        MqttConnectPacket connectPacket = transaction.ConnectPacket;
        if (connectPacket.AuthenticationMethod != null)
        {
            return await RejectConnectAsync(
                transaction,
                MqttReasonCode.BadAuthenticationMethod).ConfigureAwait(false);
        }

        if (connectPacket.HasWill &&
            connectPacket.WillProperties.PayloadFormatIndicator ==
                MqttPayloadFormatIndicator.Utf8 &&
            !MqttZeroProtocolValidator.IsValidUtf8(connectPacket.WillPayload))
        {
            return await RejectConnectAsync(
                transaction,
                MqttReasonCode.PayloadFormatInvalid).ConfigureAwait(false);
        }

        MqttZeroConnectionContext connectionContext = new()
        {
            Client = transaction.ClientInfo,
            ConnectPacket = connectPacket
        };
        foreach (IMqttZeroConnectionInterceptor interceptor in _connectionInterceptors)
        {
            MqttZeroConnectionInterceptionResult result;
            try
            {
                result = await interceptor.InterceptAsync(
                    connectionContext,
                    CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                result = MqttZeroConnectionInterceptionResult.Reject;
                connectionContext.RejectReasonCode =
                    MqttReasonCode.ImplementationSpecificError;
            }

            if (result == MqttZeroConnectionInterceptionResult.Drop)
            {
                await transaction.NewConnection.DisposeAsync().ConfigureAwait(false);
                return false;
            }

            if (result == MqttZeroConnectionInterceptionResult.Reject)
            {
                return await RejectConnectAsync(
                    transaction,
                    connectionContext.RejectReasonCode,
                    connectionContext.ReasonString).ConfigureAwait(false);
            }
        }

        return true;
    }

    /// <summary>
    /// 拒绝连接。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <param name="reasonCode">原因代码。</param>
    /// <param name="reasonString">原因字符串。</param>
    /// <returns>异步操作的结果。</returns>
    private static async ValueTask<bool> RejectConnectAsync(
        ConnectTransaction transaction,
        MqttReasonCode reasonCode,
        string? reasonString = null)
    {
        try
        {
            await transaction.NewConnection.SendConnAckAsync(
                new MqttConnAckPacket
                {
                    ReasonCode = reasonCode,
                    ReasonString = reasonString
                },
                CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            await transaction.NewConnection.DisposeAsync().ConfigureAwait(false);
        }

        return false;
    }

    /// <summary>
    /// 挂接会话。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    private void AttachSession(ConnectTransaction transaction)
    {
        MqttConnectPacket connectPacket = transaction.ConnectPacket;
        lock (_sessionGate)
        {
            _sessions.TryGetValue(
                transaction.ClientId,
                out MqttZeroSessionState? existing);
            int expectedShardId = SessionShardMap.GetShardId(
                transaction.ClientId);
            if (existing != null &&
                existing.ShardId != expectedShardId)
            {
                throw new InvalidOperationException(
                    "The MQTT session is attached to an unexpected event loop shard.");
            }

            SelectConnectionSession(transaction, existing);
            MqttZeroSessionLifecycleSnapshot lifecycle =
                transaction.Session.Lifecycle;
            MqttZeroClientConnection? currentConnection =
                lifecycle.Connection;
            if (currentConnection != null)
            {
                transaction.OldConnection = currentConnection;
                if (lifecycle.WillMessage != null &&
                    lifecycle.WillMessage.DelayInterval == 0)
                {
                    transaction.TakeoverWill =
                        lifecycle.WillMessage;
                }
            }

            MqttZeroWillMessage? willMessage =
                connectPacket.HasWill
                    ? new MqttZeroWillMessage(connectPacket)
                    : null;
            transaction.Session.UpdateLifecycle(
                _ => lifecycle with
                {
                    Connection = transaction.NewConnection,
                    CleanStart = connectPacket.CleanStart,
                    ExpiryInterval = transaction.EffectiveExpiry,
                    DisconnectedAt = null,
                    WillDueAt = null,
                    WillMessage = willMessage
                });
            RegisterSessionDeadlines(transaction.Session);
        }
    }

    /// <summary>
    /// 选择连接会话。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <param name="existing">对应的 existing。</param>
    private void SelectConnectionSession(
        ConnectTransaction transaction,
        MqttZeroSessionState? existing)
    {
        MqttConnectPacket connectPacket = transaction.ConnectPacket;
        if (existing != null &&
            (connectPacket.CleanStart ||
                IsSessionExpired(existing, transaction.Now)))
        {
            transaction.ReplacedSession = existing;
            transaction.ReplacingSession = true;
            MqttZeroSessionLifecycleSnapshot lifecycle =
                existing.Lifecycle;
            transaction.OldConnection = lifecycle.Connection;
            transaction.PreviousPumpSession = existing;
            transaction.PreviousPump = existing.TakeEgress();
            if (transaction.OldConnection != null &&
                lifecycle.WillMessage != null &&
                lifecycle.WillMessage.DelayInterval == 0)
            {
                transaction.TakeoverWill = lifecycle.WillMessage;
            }

            transaction.Session = CreateSessionState(transaction.ClientId);
            transaction.SessionPresent = false;
            return;
        }

        if (existing == null)
        {
            if (_sessions.Count >= _options.MaxSessions)
            {
                throw new InvalidOperationException(
                    "The maximum number of MQTT sessions was reached.");
            }

            transaction.Session = CreateSessionState(transaction.ClientId);
            transaction.SessionPresent = false;
            return;
        }

        transaction.Session = existing;
        transaction.SessionPresent = true;
        MqttZeroSessionLifecycleSnapshot restoreLifecycle =
            existing.Lifecycle;
        transaction.PreviousCleanStart =
            restoreLifecycle.CleanStart;
        transaction.PreviousExpiryInterval =
            restoreLifecycle.ExpiryInterval;
        transaction.PreviousDisconnectedAt =
            restoreLifecycle.DisconnectedAt;
        transaction.PreviousWillMessage =
            restoreLifecycle.WillMessage;
        transaction.PreviousWillDueAt =
            restoreLifecycle.WillDueAt;
        transaction.PreviousPumpSession = existing;
        transaction.PreviousPump = existing.TakeEgress();
    }

    /// <summary>
    /// 持久化会话。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask PersistSessionAsync(
        ConnectTransaction transaction)
    {
        if (transaction.ReplacingSession)
        {
            await DeleteClientPersistenceAsync(
                transaction.ClientId,
                CancellationToken.None).ConfigureAwait(false);
            transaction.ExistingPersistenceDeleted = true;
        }

        await SaveSessionAsync(
            transaction.Session,
            CancellationToken.None).ConfigureAwait(false);
        transaction.NewSessionPersisted = true;
        lock (_sessionGate)
        {
            InstallPersistedSession(transaction);
        }
    }

    /// <summary>
    /// InstallPersisted会话。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    private void InstallPersistedSession(ConnectTransaction transaction)
    {
        if (transaction.ReplacedSession is { } previous)
        {
            if (!_sessions.TryGetValue(
                    transaction.ClientId,
                    out MqttZeroSessionState? current) ||
                !ReferenceEquals(current, previous))
            {
                throw new InvalidOperationException(
                    "The MQTT session changed while the connection was being established.");
            }

            previous.Connection = null;

            _sessions[transaction.ClientId] = transaction.Session;
            return;
        }

        if (transaction.SessionPresent)
        {
            return;
        }

        if (_sessions.Count >= _options.MaxSessions)
        {
            throw new InvalidOperationException(
                "The maximum number of MQTT sessions was reached.");
        }

        _sessions[transaction.ClientId] = transaction.Session;
    }

    /// <summary>
    /// Activate连接。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ActivateConnectionAsync(
        ConnectTransaction transaction)
    {
        await StopEgressAsync(transaction.PreviousPump).ConfigureAwait(false);
        transaction.PreviousPumpStopped = transaction.PreviousPump != null;
        await PublishTakeoverWillAsync(transaction).ConfigureAwait(false);
        await SendSuccessfulConnAckAsync(transaction).ConfigureAwait(false);
        transaction.NewConnection.IsConnectedClientCounted = true;
        Interlocked.Increment(ref _connectedClientCount);
        await ClosePreviousConnectionAsync(transaction).ConfigureAwait(false);

        if (transaction.ReplacedSession != null)
        {
            ReleaseSessionMessagesCore(transaction.ReplacedSession);
        }

        MqttZeroConnectedClient connectedClient = new(
            this,
            transaction.Session,
            transaction.NewConnection);
        transaction.NewConnection.ConnectedClient = connectedClient;
        await NotifyConnectedAsync(
            connectedClient,
            transaction.SessionPresent).ConfigureAwait(false);

        if (transaction.ReplacingSession &&
            transaction.ReplacedSession != null)
        {
            await RemoveSessionSubscriptionsFromRouteAsync(
                transaction.ReplacedSession).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 发布Takeover遗嘱。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask PublishTakeoverWillAsync(
        ConnectTransaction transaction)
    {
        if (transaction.TakeoverWill == null)
        {
            return;
        }

        await RoutePublishAsync(
            null,
            transaction.TakeoverWill.CreatePacket(),
            CancellationToken.None).ConfigureAwait(false);
        Interlocked.Increment(ref _publishedWillCount);
    }

    /// <summary>
    /// 发送SuccessfulConn确认。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask SendSuccessfulConnAckAsync(
        ConnectTransaction transaction)
    {
        await transaction.NewConnection.SendConnAckAsync(
            new MqttConnAckPacket
            {
                SessionPresent = transaction.SessionPresent,
                ReasonCode = MqttReasonCode.Success,
                TopicAliasMaximum = _options.TopicAliasMaximum,
                ReceiveMaximum =
                    (ushort)Math.Min(_options.MaxInflight, ushort.MaxValue),
                MaximumPacketSize = (uint)_options.MaxPacketSize,
                SessionExpiryInterval = transaction.EffectiveExpiry,
                AssignedClientIdentifier =
                    transaction.AssignedClientIdentifier
                        ? transaction.ClientId
                        : null,
                ResponseInformation =
                    transaction.ConnectPacket.RequestResponseInformation
                        ? _options.ResponseInformation
                        : null
            },
            CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// 关闭前一个连接。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask ClosePreviousConnectionAsync(
        ConnectTransaction transaction)
    {
        if (transaction.OldConnection == null ||
            ReferenceEquals(
                transaction.OldConnection,
                transaction.NewConnection))
        {
            return;
        }

        try
        {
            await transaction.OldConnection.CloseAsync(
                MqttReasonCode.SessionTakenOver,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogConnectionCleanupFailure(
                _logger,
                exception);
        }
    }

    /// <summary>
    /// 通知Connected。
    /// </summary>
    /// <param name="connectedClient">connected客户端。</param>
    /// <param name="sessionPresent">会话Present。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask NotifyConnectedAsync(
        MqttZeroConnectedClient connectedClient,
        bool sessionPresent)
    {
        MqttZeroClientConnectedContext connectedContext = new()
        {
            ConnectedClient = connectedClient,
            SessionPresent = sessionPresent
        };
        foreach (IMqttZeroConnectionObserver observer in _connectionObservers)
        {
            try
            {
                await observer.OnConnectedAsync(
                    connectedContext,
                    CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogObserverFailure(_logger, exception);
            }
        }
    }

    /// <summary>
    /// RollbackTransaction。
    /// </summary>
    /// <param name="transaction">对应的 transaction。</param>
    /// <param name="registerDeadlines">对应的 registerDeadlines。</param>
    /// <param name="removeDeadlines">对应的 removeDeadlines。</param>
    /// <param name="persistRestoredSession">persistRestored会话。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RollbackTransactionAsync(
        ConnectTransaction transaction,
        bool registerDeadlines,
        bool removeDeadlines,
        bool persistRestoredSession)
    {
        await RollbackConnectionAsync(
            transaction.ClientId,
            transaction.Session,
            transaction.SessionPresent,
            transaction.NewSessionPersisted,
            transaction.ExistingPersistenceDeleted,
            CreateRestoreState(
                transaction.ReplacedSession,
                transaction.OldConnection,
                transaction.PreviousPumpSession,
                transaction.PreviousPump,
                transaction.PreviousPumpStopped,
                transaction.PreviousCleanStart,
                transaction.PreviousExpiryInterval,
                transaction.PreviousDisconnectedAt,
                transaction.PreviousWillMessage,
                transaction.PreviousWillDueAt),
            registerDeadlines,
            removeDeadlines,
            persistRestoredSession).ConfigureAwait(false);
    }

    /// <summary>
    /// CancelAccepted连接。
    /// </summary>
    /// <param name="state">状态。</param>
    private static void CancelAcceptedConnection(object? state)
    {
        if (state is CancellationTokenSource source)
        {
            source.Cancel();
        }
    }

    /// <summary>
    /// 创建Restore状态。
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
    /// <returns>对应的结果。</returns>
    private static MqttZeroSessionRestoreState CreateRestoreState(
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
        return new MqttZeroSessionRestoreState(
            replacedSession,
            oldConnection,
            previousPumpSession,
            previousPump,
            previousPumpStopped,
            previousCleanStart,
            previousExpiryInterval,
            previousDisconnectedAt,
            previousWillMessage,
            previousWillDueAt);
    }

    /// <summary>
    /// Rollback连接。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="session">会话。</param>
    /// <param name="sessionPresent">会话Present。</param>
    /// <param name="newSessionPersisted">new会话Persisted。</param>
    /// <param name="existingPersistenceDeleted">对应的 existingPersistenceDeleted。</param>
    /// <param name="restore">对应的 restore。</param>
    /// <param name="registerDeadlines">对应的 registerDeadlines。</param>
    /// <param name="removeDeadlines">对应的 removeDeadlines。</param>
    /// <param name="persistRestoredSession">persistRestored会话。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RollbackConnectionAsync(
        string clientId,
        MqttZeroSessionState session,
        bool sessionPresent,
        bool newSessionPersisted,
        bool existingPersistenceDeleted,
        MqttZeroSessionRestoreState restore,
        bool registerDeadlines,
        bool removeDeadlines,
        bool persistRestoredSession)
    {
        if (restore.ReplacedSession is { } replacedSession)
        {
            await RollbackReplacedSessionAsync(
                clientId,
                session,
                existingPersistenceDeleted,
                replacedSession,
                restore,
                registerDeadlines).ConfigureAwait(false);
            return;
        }

        if (!sessionPresent)
        {
            await RollbackNewSessionAsync(
                clientId,
                session,
                newSessionPersisted,
                removeDeadlines).ConfigureAwait(false);
            return;
        }

        await RollbackExistingSessionAsync(
            clientId,
            session,
            restore,
            persistRestoredSession).ConfigureAwait(false);
    }

    /// <summary>
    /// RollbackReplaced会话。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="session">会话。</param>
    /// <param name="existingPersistenceDeleted">对应的 existingPersistenceDeleted。</param>
    /// <param name="replacedSession">replaced会话。</param>
    /// <param name="restore">对应的 restore。</param>
    /// <param name="registerDeadlines">对应的 registerDeadlines。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RollbackReplacedSessionAsync(
        string clientId,
        MqttZeroSessionState session,
        bool existingPersistenceDeleted,
        MqttZeroSessionState replacedSession,
        MqttZeroSessionRestoreState restore,
        bool registerDeadlines)
    {
        lock (_sessionGate)
        {
            if (_sessions.TryGetValue(
                    clientId,
                    out MqttZeroSessionState? current) &&
                ReferenceEquals(current, session))
            {
                _sessions[clientId] = replacedSession;
            }
        }

        replacedSession.UpdateLifecycle(
            current => current with
            {
                Connection = restore.OldConnection
            });

        if (registerDeadlines)
        {
            RegisterSessionDeadlines(replacedSession);
        }

        RestoreSessionEgress(
            restore.PreviousPumpSession,
            restore.OldConnection,
            restore.PreviousPump,
            restore.PreviousPumpStopped);
        if (!existingPersistenceDeleted)
        {
            return;
        }

        try
        {
            await RestoreSessionPersistenceAsync(
                replacedSession,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(RestoreSessionPersistenceAsync),
                clientId,
                exception);
        }
    }

    /// <summary>
    /// RollbackNew会话。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="session">会话。</param>
    /// <param name="newSessionPersisted">new会话Persisted。</param>
    /// <param name="removeDeadlines">对应的 removeDeadlines。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RollbackNewSessionAsync(
        string clientId,
        MqttZeroSessionState session,
        bool newSessionPersisted,
        bool removeDeadlines)
    {
        lock (_sessionGate)
        {
            _sessions.TryRemove(session.ClientId, out _);
        }

        if (removeDeadlines)
        {
            RemoveSessionDeadlines(session);
        }

        ReleaseSessionMessagesCore(session);
        if (!newSessionPersisted)
        {
            return;
        }

        try
        {
            await DeleteSessionAsync(
                clientId,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(IMqttZeroSessionStore.DeleteAsync),
                clientId,
                exception);
        }
    }

    /// <summary>
    /// RollbackExisting会话。
    /// </summary>
    /// <param name="clientId">客户端Id。</param>
    /// <param name="session">会话。</param>
    /// <param name="restore">对应的 restore。</param>
    /// <param name="persistRestoredSession">persistRestored会话。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask RollbackExistingSessionAsync(
        string clientId,
        MqttZeroSessionState session,
        MqttZeroSessionRestoreState restore,
        bool persistRestoredSession)
    {
        session.UpdateLifecycle(
            current => current with
            {
                Connection = restore.OldConnection,
                CleanStart = restore.PreviousCleanStart,
                ExpiryInterval = restore.PreviousExpiryInterval,
                DisconnectedAt = restore.PreviousDisconnectedAt,
                WillMessage = restore.PreviousWillMessage,
                WillDueAt = restore.PreviousWillDueAt
            });

        RestoreSessionEgress(
            restore.PreviousPumpSession,
            restore.OldConnection,
            restore.PreviousPump,
            restore.PreviousPumpStopped);
        if (!persistRestoredSession)
        {
            return;
        }

        try
        {
            await SaveSessionAsync(session, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            MqttZeroBrokerLogging.LogPersistenceFailure(
                _logger,
                nameof(SaveSessionAsync),
                clientId,
                exception);
        }
    }

    /// <summary>
    /// 恢复会话出站。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="pump">对应的 pump。</param>
    /// <param name="pumpStopped">对应的 pumpStopped。</param>
    private void RestoreSessionEgress(
        MqttZeroSessionState? session,
        MqttZeroClientConnection? connection,
        MqttZeroSessionEgress? pump,
        bool pumpStopped)
    {
        if (session == null || connection == null)
        {
            return;
        }

        if (!pumpStopped && pump != null)
        {
            if (!session.TryAttachEgress(connection, pump))
            {
                return;
            }

            pump.Signal();
            return;
        }

        if (!connection.ClosedToken.IsCancellationRequested)
        {
            try
            {
                StartEgress(
                    session,
                    connection,
                    connection.ClosedToken);
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogEgressFailure(
                    _logger,
                    session.ClientId,
                    exception);
                connection.Abort();
            }
        }
    }

    /// <summary>
    /// 释放Connected客户端数量。
    /// </summary>
    /// <param name="connection">连接。</param>
    private void ReleaseConnectedClientCount(MqttZeroClientConnection connection)
    {
        if (!connection.IsConnectedClientCounted)
        {
            return;
        }

        connection.IsConnectedClientCounted = false;
        Interlocked.Decrement(ref _connectedClientCount);
    }

    /// <summary>
    /// CONNECT 完成后挂到会话上的连接信息。
    /// </summary>
    private sealed class ConnectionAttachment
    {
        /// <summary>
        /// 初始化 ConnectionAttachment 的新实例。
        /// </summary>
        /// <param name="session">会话。</param>
        /// <param name="connection">连接。</param>
        public ConnectionAttachment(
            MqttZeroSessionState session,
            MqttZeroClientConnection connection)
        {
            Session = session;
            Connection = connection;
        }

        /// <summary>
        /// 会话。
        /// </summary>
        public MqttZeroSessionState Session { get; }

        /// <summary>
        /// 连接。
        /// </summary>
        public MqttZeroClientConnection Connection { get; }
    }

    /// <summary>
    /// Own入站租约。
    /// </summary>
    /// <param name="lease">租约。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    private async ValueTask OwnInboundLeaseAsync(
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                lease.EnsureOwned();
                return;
            }
            catch (MqttZeroBufferPoolExhaustedException exhaustion)
            {
                await _bufferPool.WaitForCapacityAsync(
                    exhaustion.ReturnGeneration,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

}
