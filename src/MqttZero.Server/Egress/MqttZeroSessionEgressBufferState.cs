using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Diagnostics;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroSessionEgressBufferState
{
    internal List<MqttZeroQueuedMessage> ExpiredMessages { get; } = [];

    internal List<MqttZeroQueuedMessage> OutboundBatch { get; } = [];

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
            if (headTooLarge ||
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
