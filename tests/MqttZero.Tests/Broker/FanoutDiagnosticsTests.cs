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
}
