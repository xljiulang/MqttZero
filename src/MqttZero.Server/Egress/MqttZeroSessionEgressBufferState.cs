using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Diagnostics;

namespace MqttZero.Server.Egress;

/// <summary>
/// 出站批次复用的缓冲区状态。
/// </summary>
internal sealed class MqttZeroSessionEgressBufferState
{
    /// <summary>
    /// Expired消息。
    /// </summary>
    internal List<MqttZeroQueuedMessage> ExpiredMessages { get; } = [];

    /// <summary>
    /// 出站批次。
    /// </summary>
    internal List<MqttZeroQueuedMessage> OutboundBatch { get; } = [];

    /// <summary>
    /// 选择批次。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="connection">连接。</param>
    /// <param name="options">选项。</param>
    /// <param name="timeProvider">时间Provider。</param>
    /// <param name="diagnostics">诊断。</param>
    /// <param name="budget">预算。</param>
    /// <param name="next">下一个。</param>
    /// <param name="hasNext">has下一个。</param>
    /// <param name="nextTooLarge">下一个TooLarge。</param>
    /// <param name="inflightLimit">在途上限。</param>
    internal void SelectBatch(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroOptions options,
        TimeProvider timeProvider,
        MqttZeroFanoutDiagnostics diagnostics,
        ref MqttZeroEgressBatchBudget budget,
        out MqttZeroQueuedMessage? next,
        out bool hasNext,
        out bool nextTooLarge,
        out int inflightLimit)
    {
        next = null;
        hasNext = false;
        nextTooLarge = false;
        inflightLimit = Math.Min(
            options.MaxInflight,
            connection.ReceiveMaximum);
        bool diagnosticsEnabled = diagnostics.Enabled;
        long pendingSelectionStarted = diagnosticsEnabled
            ? Stopwatch.GetTimestamp()
            : 0;
        ExpiredMessages.Clear();
        OutboundBatch.Clear();
        DateTimeOffset now = timeProvider.GetUtcNow();
        session.PendingMessages.RemoveExpired(
            now,
            ExpiredMessages);
        hasNext = session.PendingMessages.Count > 0 &&
            (session.PendingMessages[0].QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtMostOnce ||
            session.InflightMessages.Count < inflightLimit);
        if (!hasNext)
        {
            return;
        }

        next = session.PendingMessages.Dequeue();
        ValidateEncodedLength(
            connection,
            next,
            out nextTooLarge);
        if (nextTooLarge)
        {
            return;
        }

        _ = budget.TryAdd(
            next.EncodedLength,
            timeProvider.GetTimestamp());
        OutboundBatch.Add(next);
        AddInflightIfNeeded(
            session,
            next,
            diagnostics,
            diagnosticsEnabled);

        while (!connection.HasPendingControlWrites &&
            session.PendingMessages.Count > 0)
        {
            MqttZeroQueuedMessage head = session.PendingMessages[0];
            if (head.QualityOfServiceLevel !=
                    MqttQualityOfServiceLevel.AtMostOnce &&
                session.InflightMessages.Count >= inflightLimit)
            {
                break;
            }

            ValidateEncodedLength(
                connection,
                head,
                out bool headTooLarge);
            if (headTooLarge)
            {
                break;
            }

            if (OutboundBatch.Count > 0 &&
                MqttZeroEgressBatchPolicy.IsLarge(head.EncodedLength) &&
                !MqttZeroEgressBatchPolicy.IsLarge(
                    OutboundBatch[0].EncodedLength))
            {
                break;
            }

            if (
                !budget.TryAdd(
                    head.EncodedLength,
                    timeProvider.GetTimestamp()))
            {
                break;
            }

            MqttZeroQueuedMessage candidate =
                session.PendingMessages.Dequeue();
            OutboundBatch.Add(candidate);
            AddInflightIfNeeded(
                session,
                candidate,
                diagnostics,
                diagnosticsEnabled);
        }

        if (diagnosticsEnabled)
        {
            diagnostics.RecordPendingSelection(
                Stopwatch.GetTimestamp() - pendingSelectionStarted);
        }
    }

    /// <summary>
    /// 添加在途IfNeeded。
    /// </summary>
    /// <param name="session">会话。</param>
    /// <param name="message">消息。</param>
    /// <param name="diagnostics">诊断。</param>
    /// <param name="diagnosticsEnabled">诊断Enabled。</param>
    private static void AddInflightIfNeeded(
        MqttZeroSessionState session,
        MqttZeroQueuedMessage message,
        MqttZeroFanoutDiagnostics diagnostics,
        bool diagnosticsEnabled)
    {
        if (message.QualityOfServiceLevel ==
            MqttQualityOfServiceLevel.AtMostOnce)
        {
            return;
        }

        long inflightStarted = diagnosticsEnabled
            ? Stopwatch.GetTimestamp()
            : 0;
        message.PacketIdentifier = session.GetNextPacketIdentifier();
        session.InflightMessages[message.PacketIdentifier] = message;
        if (diagnosticsEnabled)
        {
            diagnostics.RecordInflightUpdate(
                Stopwatch.GetTimestamp() - inflightStarted);
        }
    }

    /// <summary>
    /// 释放QosZero消息。
    /// </summary>
    /// <param name="batch">批次。</param>
    internal static void ReleaseQosZeroMessages(
        List<MqttZeroQueuedMessage> batch)
    {
        for (int index = 0; index < batch.Count; index++)
        {
            MqttZeroQueuedMessage message = batch[index];
            if (message.QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtMostOnce)
            {
                message.Release();
            }
        }
    }

    /// <summary>
    /// 释放停止过程中已从会话队列取出的消息。
    /// </summary>
    internal void ReleaseStoppedMessages()
    {
        for (int index = 0; index < ExpiredMessages.Count; index++)
        {
            ExpiredMessages[index].Release();
        }

        ExpiredMessages.Clear();
        ReleaseQosZeroMessages(OutboundBatch);
        OutboundBatch.Clear();
    }

    /// <summary>
    /// 校验Encoded长度。
    /// </summary>
    /// <param name="connection">连接。</param>
    /// <param name="message">消息。</param>
    /// <param name="tooLarge">对应的 tooLarge。</param>
    private static void ValidateEncodedLength(
        MqttZeroClientConnection connection,
        MqttZeroQueuedMessage message,
        out bool tooLarge)
    {
        if (!message.IsEncodedLengthValidFor(connection.MaximumPacketSize))
        {
            int encodedLength = connection.GetPublishEncodedLength(
                message.Lease,
                message.QualityOfServiceLevel,
                message.Retain,
                message.SubscriptionIdentifiers);
            message.SetEncodedLength(
                encodedLength,
                connection.MaximumPacketSize);
        }

        tooLarge = message.EncodedLength > connection.MaximumPacketSize;
    }
}
