using MqttZero.BrokerBenchmarks;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkObservationCaptureTests
{
    [Fact]
    public void EnvironmentCaptureReturnsStableRuntimeIdentity()
    {
        BenchmarkRuntimeSnapshot runtime = new BenchmarkRuntimeConfiguration(4)
            .ApplyToCurrentProcess();
        BenchmarkTopologyInfo topology =
        new BenchmarkTopologyOptions(4, 2).CreateInfo();
        BenchmarkEgressOptions egress = new(
            OutboundBatchBytes: 256 * 1024,
            OutboundBatchMessages: 64,
            OutboundBatchDurationMicroseconds: 250);

        BenchmarkEnvironmentInfo environment =
            BenchmarkObservationCapture.CaptureEnvironment(
                runtime,
                topology,
                egress);

        Assert.False(string.IsNullOrWhiteSpace(environment.OsDescription));
        Assert.False(string.IsNullOrWhiteSpace(environment.OsVersion));
        Assert.False(string.IsNullOrWhiteSpace(environment.RuntimeVersion));
        Assert.False(string.IsNullOrWhiteSpace(environment.ProcessArchitecture));
        Assert.False(string.IsNullOrWhiteSpace(environment.RuntimeIdentifier));
        Assert.True(environment.ProcessorCount > 0);
        Assert.Equal(runtime, environment.Runtime);
        Assert.Equal(topology, environment.Topology);
        Assert.Equal(egress, environment.Egress);
    }

    [Fact]
    public void LoadDeltaCountsAreNonNegativeAndAllocationsIncrease()
    {
        BenchmarkLoadSnapshot before = BenchmarkObservationCapture.CreateLoadSnapshot();
        byte[] ignored = GC.AllocateUninitializedArray<byte>(4096, pinned: true);
        Assert.NotEmpty(ignored);

        BenchmarkLoadObservation observation =
            BenchmarkObservationCapture.CaptureLoadDelta(before);

        Assert.True(observation.ProcessCpuMilliseconds >= 0);
        Assert.True(observation.WorkingSetBytes >= 0);
        Assert.True(observation.AllocatedBytesDelta > 0);
        Assert.True(observation.Gen0CollectionCountDelta >= 0);
        Assert.True(observation.Gen1CollectionCountDelta >= 0);
        Assert.True(observation.Gen2CollectionCountDelta >= 0);
        Assert.True(observation.ThreadPoolPendingWorkItemCount >= 0);
        Assert.True(observation.ThreadPoolCompletedWorkItemCountDelta >= 0);
    }

    [Fact]
    public void ProcessCaptureReturnsNonNegativeResourceValues()
    {
        using System.Diagnostics.Process process =
            System.Diagnostics.Process.GetCurrentProcess();

        BenchmarkProcessObservation observation =
            BenchmarkObservationCapture.CaptureProcess(
                process,
                TimeSpan.FromMilliseconds(250));

        Assert.True(observation.TotalProcessorMilliseconds >= 0);
        Assert.True(observation.PrivateMemoryBytes > 0);
        Assert.True(observation.PeakWorkingSetBytes > 0);
        Assert.Equal(250, observation.UptimeMilliseconds);
    }
}
