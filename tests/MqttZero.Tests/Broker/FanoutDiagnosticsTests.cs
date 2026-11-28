using MqttZero.Server.Broker;

namespace MqttZero.Tests.Broker;

public sealed class FanoutDiagnosticsTests
{
    [Fact]
    public void SnapshotCountsOnePublishAndItsBatches()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

        diagnostics.RecordPublish(
            localBatchCount: 1,
            remoteBatchCount: 7,
            deliveryCount: 100,
            elapsedTimestampTicks: 50);

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();

        Assert.Equal(1, snapshot.PublishCount);
        Assert.Equal(1, snapshot.LocalBatchCount);
        Assert.Equal(7, snapshot.RemoteBatchCount);
        Assert.Equal(100, snapshot.DeliveryCount);
        Assert.Equal(50, snapshot.TotalFanoutTicks);
        Assert.Equal(50, snapshot.MaxFanoutTicks);
    }

    [Fact]
    public void DisabledDiagnosticsDoNotRecord()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(false);

        diagnostics.RecordPublish(1, 7, 100, 50);
        diagnostics.RecordEgressSignal();
        diagnostics.RecordEgressDrain();
        diagnostics.RecordEncodedPublishes(100);
        diagnostics.RecordFlushWait();
        diagnostics.RecordPendingQueueDepth(1000);
        diagnostics.RecordEgressSignalWithoutPending();
        diagnostics.RecordEgressReadyToExecute(10);
        diagnostics.RecordEgressEmptyDataDrain();
        diagnostics.RecordEgressSynchronousFlush();

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();

        Assert.Equal(0, snapshot.PublishCount);
        Assert.Equal(0, snapshot.EgressSignalCount);
        Assert.Equal(0, snapshot.EgressSignalWithoutPendingCount);
        Assert.Equal(0, snapshot.EgressReadyToExecuteCount);
        Assert.Equal(0, snapshot.EgressEmptyDataDrainCount);
        Assert.Equal(0, snapshot.EgressSynchronousFlushCount);
        Assert.Equal(0, snapshot.MaxPendingQueueDepth);
    }

    [Fact]
    public void DiagnosticsTrackEncodedBytesUntilFlushCompletes()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

        diagnostics.RecordEgressFlushStarted(64 * 1024);

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();
        Assert.Equal(1, snapshot.CurrentEgressFlushPendingCount);
        Assert.Equal(
            64 * 1024,
            snapshot.CurrentEncodedBytesAwaitingFlush);
        Assert.Equal(1, snapshot.MaxEgressFlushPendingCount);
        Assert.Equal(
            64 * 1024,
            snapshot.MaxEncodedBytesAwaitingFlush);

        diagnostics.RecordEgressFlushCompleted(64 * 1024);

        snapshot = diagnostics.GetSnapshot();
        Assert.Equal(0, snapshot.CurrentEgressFlushPendingCount);
        Assert.Equal(0, snapshot.CurrentEncodedBytesAwaitingFlush);
    }

    [Fact]
    public void DiagnosticsTrackMaximumObservedQueueDepths()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

        diagnostics.RecordPendingQueueDepth(7);
        diagnostics.RecordInflightQueueDepth(3);

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();
        Assert.Equal(7, snapshot.MaxObservedSessionPendingQueueDepth);
        Assert.Equal(3, snapshot.MaxObservedSessionInflightQueueDepth);
    }

    [Fact]
    public void DiagnosticsTrackEmptyEgressAndSchedulerWait()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

        diagnostics.RecordEgressSignalWithoutPending();
        diagnostics.RecordEgressReadyToExecute(40);
        diagnostics.RecordEgressReadyToExecute(7);
        diagnostics.RecordEgressEmptyDataDrain();
        diagnostics.RecordEgressSynchronousFlush();

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();
        Assert.Equal(1, snapshot.EgressSignalWithoutPendingCount);
        Assert.Equal(2, snapshot.EgressReadyToExecuteCount);
        Assert.Equal(47, snapshot.TotalEgressReadyToExecuteTicks);
        Assert.Equal(40, snapshot.MaxEgressReadyToExecuteTicks);
        Assert.Equal(1, snapshot.EgressEmptyDataDrainCount);
        Assert.Equal(1, snapshot.EgressSynchronousFlushCount);
    }

    [Fact]
    public void SnapshotCountsEgressSchedulerAndBatchMetrics()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

        diagnostics.RecordEgressScheduledActor();
        diagnostics.RecordEgressSchedulerPass(actorCount: 3, 20);
        diagnostics.RecordEgressDrain();
        diagnostics.RecordEgressDrainTiming(15);
        diagnostics.RecordEgressDataBatch(messageCount: 8, encodedBytes: 4096);
        diagnostics.RecordEgressFlushStart();

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();

        Assert.Equal(1, snapshot.EgressScheduledActorCount);
        Assert.Equal(1, snapshot.EgressSchedulerPassCount);
        Assert.Equal(3, snapshot.EgressScheduledActorPassCount);
        Assert.Equal(20, snapshot.TotalEgressSchedulerTicks);
        Assert.Equal(1, snapshot.EgressDrainCount);
        Assert.Equal(15, snapshot.TotalEgressDrainTicks);
        Assert.Equal(15, snapshot.MaxEgressDrainTicks);
        Assert.Equal(1, snapshot.EgressDataBatchCount);
        Assert.Equal(8, snapshot.EgressBatchMessageCount);
        Assert.Equal(4096, snapshot.EgressBatchByteCount);
        Assert.Equal(1, snapshot.EgressFlushStartCount);
    }

    [Fact]
    public void SnapshotCountsFanoutSchedulerMetrics()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

        diagnostics.RecordFanoutSchedulerEnqueue(25);
        diagnostics.RecordFanoutSchedulerPass(batchCount: 4);
        diagnostics.RecordFanoutSchedulerFailure();
        diagnostics.RecordFanoutFailure();

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();

        Assert.Equal(1, snapshot.FanoutSchedulerEnqueueCount);
        Assert.Equal(1, snapshot.FanoutSchedulerPassCount);
        Assert.Equal(4, snapshot.FanoutSchedulerBatchCount);
        Assert.Equal(1, snapshot.FanoutSchedulerFailureCount);
        Assert.Equal(1, snapshot.FanoutFailureCount);
        Assert.Equal(
            25d * 1_000_000d / System.Diagnostics.Stopwatch.Frequency,
            snapshot.MaxFanoutSchedulerWaitMicroseconds);
    }

    [Fact]
    public void SnapshotCountsSendStageAttribution()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

        diagnostics.RecordPendingSelection(12);
        diagnostics.RecordInflightUpdate(13);
        diagnostics.RecordPublishEncode(14);
        diagnostics.RecordPublishWrite(15);

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();

        Assert.Equal(12, snapshot.TotalPendingSelectionTicks);
        Assert.Equal(13, snapshot.TotalInflightUpdateTicks);
        Assert.Equal(14, snapshot.TotalPublishEncodeTicks);
        Assert.Equal(15, snapshot.TotalPublishWriteTicks);
    }

    [Fact]
    public void SnapshotCountsShardOwnershipViolations()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);

        diagnostics.RecordShardOwnershipViolation();

        Assert.Equal(
            1,
            diagnostics.GetSnapshot().ShardOwnershipViolationCount);
    }

    [Fact]
    public void ResetClearsAllDiagnostics()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);
        diagnostics.RecordPublish(1, 2, 3, 4);
        diagnostics.RecordEgressSignal();
        diagnostics.RecordEgressSignalWithoutPending();
        diagnostics.RecordEgressReadyToExecute(2);
        diagnostics.RecordEgressEmptyDataDrain();
        diagnostics.RecordEgressSynchronousFlush();
        diagnostics.RecordEgressScheduledActor();
        diagnostics.RecordEgressSchedulerPass(1, 2);
        diagnostics.RecordEgressDrain();
        diagnostics.RecordEgressDrainTiming(3);
        diagnostics.RecordEgressDataBatch(4, 5);
        diagnostics.RecordEgressFlushStart();
        diagnostics.RecordFlushWait();
        diagnostics.RecordPendingQueueDepth(6);
        diagnostics.RecordInflightQueueDepth(5);
        diagnostics.RecordEgressFlushStarted(1024);
        diagnostics.RecordFanoutSchedulerEnqueue(7);
        diagnostics.RecordFanoutSchedulerPass(8);
        diagnostics.RecordFanoutSchedulerFailure();
        diagnostics.RecordFanoutFailure();
        diagnostics.RecordShardOwnershipViolation();

        diagnostics.Reset();
        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();

        Assert.Equal(0, snapshot.PublishCount);
        Assert.Equal(0, snapshot.EgressSignalCount);
        Assert.Equal(0, snapshot.EgressSignalWithoutPendingCount);
        Assert.Equal(0, snapshot.EgressReadyToExecuteCount);
        Assert.Equal(0, snapshot.EgressEmptyDataDrainCount);
        Assert.Equal(0, snapshot.EgressSynchronousFlushCount);
        Assert.Equal(0, snapshot.EgressScheduledActorCount);
        Assert.Equal(0, snapshot.EgressDrainCount);
        Assert.Equal(0, snapshot.EgressDataBatchCount);
        Assert.Equal(0, snapshot.EgressFlushStartCount);
        Assert.Equal(0, snapshot.FlushWaitCount);
        Assert.Equal(0, snapshot.MaxPendingQueueDepth);
        Assert.Equal(0, snapshot.MaxObservedSessionPendingQueueDepth);
        Assert.Equal(0, snapshot.MaxObservedSessionInflightQueueDepth);
        Assert.Equal(0, snapshot.CurrentEgressFlushPendingCount);
        Assert.Equal(0, snapshot.CurrentEncodedBytesAwaitingFlush);
        Assert.Equal(0, snapshot.FanoutSchedulerEnqueueCount);
        Assert.Equal(0, snapshot.FanoutSchedulerPassCount);
        Assert.Equal(0, snapshot.FanoutSchedulerBatchCount);
        Assert.Equal(0, snapshot.FanoutSchedulerFailureCount);
        Assert.Equal(0, snapshot.FanoutFailureCount);
        Assert.Equal(0, snapshot.MaxFanoutSchedulerWaitMicroseconds);
        Assert.Equal(0, snapshot.ShardOwnershipViolationCount);
    }
}
