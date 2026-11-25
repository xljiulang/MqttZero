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

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();

        Assert.Equal(0, snapshot.PublishCount);
        Assert.Equal(0, snapshot.EgressSignalCount);
        Assert.Equal(0, snapshot.MaxPendingQueueDepth);
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
    public void ResetClearsAllDiagnostics()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(enabled: true);
        diagnostics.RecordPublish(1, 2, 3, 4);
        diagnostics.RecordEgressSignal();
        diagnostics.RecordEgressScheduledActor();
        diagnostics.RecordEgressSchedulerPass(1, 2);
        diagnostics.RecordEgressDrain();
        diagnostics.RecordEgressDrainTiming(3);
        diagnostics.RecordEgressDataBatch(4, 5);
        diagnostics.RecordEgressFlushStart();
        diagnostics.RecordFlushWait();
        diagnostics.RecordPendingQueueDepth(6);
        diagnostics.RecordFanoutSchedulerEnqueue(7);
        diagnostics.RecordFanoutSchedulerPass(8);
        diagnostics.RecordFanoutSchedulerFailure();
        diagnostics.RecordFanoutFailure();

        diagnostics.Reset();
        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();

        Assert.Equal(0, snapshot.PublishCount);
        Assert.Equal(0, snapshot.EgressSignalCount);
        Assert.Equal(0, snapshot.EgressScheduledActorCount);
        Assert.Equal(0, snapshot.EgressDrainCount);
        Assert.Equal(0, snapshot.EgressDataBatchCount);
        Assert.Equal(0, snapshot.EgressFlushStartCount);
        Assert.Equal(0, snapshot.FlushWaitCount);
        Assert.Equal(0, snapshot.MaxPendingQueueDepth);
        Assert.Equal(0, snapshot.FanoutSchedulerEnqueueCount);
        Assert.Equal(0, snapshot.FanoutSchedulerPassCount);
        Assert.Equal(0, snapshot.FanoutSchedulerBatchCount);
        Assert.Equal(0, snapshot.FanoutSchedulerFailureCount);
        Assert.Equal(0, snapshot.FanoutFailureCount);
        Assert.Equal(0, snapshot.MaxFanoutSchedulerWaitMicroseconds);
    }
}
