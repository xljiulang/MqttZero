using MqttZero.Server.Broker;

namespace MqttZero.Tests.Broker;

public sealed class MqttZeroFanoutDiagnosticsTests
{
    [Fact]
    public void DisabledDiagnosticsIgnoreEveryRecord()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(false);

        diagnostics.RecordControlFrame(direct: true);
        diagnostics.RecordControlFrame(direct: false);
        diagnostics.RecordControlBatch(2, 100);
        diagnostics.RecordAcknowledgement();
        diagnostics.RecordAcknowledgementQueue(
            System.Diagnostics.Stopwatch.GetTimestamp());
        diagnostics.RecordQosZeroRoute(direct: true);
        diagnostics.RecordQosZeroRoute(direct: false);

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();
        Assert.Equal(0, snapshot.ControlFrameCount);
        Assert.Equal(0, snapshot.ControlFrameDirectCount);
        Assert.Equal(0, snapshot.ControlFrameQueuedCount);
        Assert.Equal(0, snapshot.ControlBatchCount);
        Assert.Equal(0, snapshot.ControlBatchFrameSum);
        Assert.Equal(0, snapshot.ControlBatchFrameMax);
        Assert.Equal(0, snapshot.TotalControlWriteTicks);
        Assert.Equal(0, snapshot.MaxControlWriteTicks);
        Assert.Equal(0, snapshot.AckWorkCount);
        Assert.Equal(0, snapshot.TotalAckWorkQueueTicks);
        Assert.Equal(0, snapshot.MaxAckWorkQueueTicks);
        Assert.Equal(0, snapshot.QosZeroDirectCount);
        Assert.Equal(0, snapshot.QosZeroDeferredCount);
    }

    [Fact]
    public void EnabledDiagnosticsAggregateControlAndAcknowledgementWork()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(true);

        diagnostics.RecordControlFrame(direct: true);
        diagnostics.RecordControlFrame(direct: false);
        diagnostics.RecordControlFrame(direct: false);
        diagnostics.RecordControlBatch(2, 100);
        diagnostics.RecordControlBatch(1, 50);
        diagnostics.RecordAcknowledgement();
        diagnostics.RecordAcknowledgement();
        diagnostics.RecordAcknowledgementQueue(
            System.Diagnostics.Stopwatch.GetTimestamp());
        diagnostics.RecordQosZeroRoute(direct: true);
        diagnostics.RecordQosZeroRoute(direct: false);

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();
        Assert.Equal(3, snapshot.ControlFrameCount);
        Assert.Equal(1, snapshot.ControlFrameDirectCount);
        Assert.Equal(2, snapshot.ControlFrameQueuedCount);
        Assert.Equal(2, snapshot.ControlBatchCount);
        Assert.Equal(3, snapshot.ControlBatchFrameSum);
        Assert.Equal(2, snapshot.ControlBatchFrameMax);
        Assert.Equal(150, snapshot.TotalControlWriteTicks);
        Assert.Equal(100, snapshot.MaxControlWriteTicks);
        Assert.Equal(2, snapshot.AckWorkCount);
        Assert.Equal(1, snapshot.QosZeroDirectCount);
        Assert.Equal(1, snapshot.QosZeroDeferredCount);
    }

    [Fact]
    public void ResetClearsTheControlCounters()
    {
        MqttZeroFanoutDiagnostics diagnostics = new(true);
        diagnostics.RecordControlFrame(direct: true);
        diagnostics.RecordControlBatch(1, 10);
        diagnostics.RecordAcknowledgement();

        diagnostics.Reset();

        MqttZeroFanoutDiagnosticsSnapshot snapshot =
            diagnostics.GetSnapshot();
        Assert.Equal(0, snapshot.ControlFrameCount);
        Assert.Equal(0, snapshot.ControlBatchCount);
        Assert.Equal(0, snapshot.AckWorkCount);
    }
}
