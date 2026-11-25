using System.Diagnostics;
using System.Runtime.InteropServices;

namespace MqttZero.BrokerBenchmarks;

internal static class BenchmarkObservationCapture
{
    public static BenchmarkEnvironmentInfo CaptureEnvironment(
        BenchmarkRuntimeSnapshot runtime,
        BenchmarkTopologyInfo topology,
        BenchmarkEgressOptions egress)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentNullException.ThrowIfNull(egress);
        return new BenchmarkEnvironmentInfo(
            RuntimeInformation.OSDescription,
            Environment.OSVersion.VersionString,
            RuntimeInformation.FrameworkDescription,
            RuntimeInformation.ProcessArchitecture.ToString(),
            RuntimeInformation.RuntimeIdentifier,
            Environment.ProcessorCount,
            runtime,
            topology,
            egress);
    }

    public static BenchmarkProcessObservation CaptureProcess(
        Process process,
        TimeSpan uptime)
    {
        ArgumentNullException.ThrowIfNull(process);
        process.Refresh();
        return new BenchmarkProcessObservation(
            process.TotalProcessorTime.TotalMilliseconds,
            process.PrivateMemorySize64,
            process.PeakWorkingSet64,
            uptime.TotalMilliseconds);
    }

    public static BenchmarkLoadSnapshot CreateLoadSnapshot()
    {
        using Process process = Process.GetCurrentProcess();
        return new BenchmarkLoadSnapshot(
            process.TotalProcessorTime.TotalMilliseconds,
            process.WorkingSet64,
            GC.GetTotalAllocatedBytes(precise: true),
            GC.CollectionCount(0),
            GC.CollectionCount(1),
            GC.CollectionCount(2),
            ThreadPool.PendingWorkItemCount,
            ThreadPool.CompletedWorkItemCount);
    }

    public static BenchmarkLoadObservation CaptureLoadDelta(
        BenchmarkLoadSnapshot before)
    {
        ArgumentNullException.ThrowIfNull(before);
        BenchmarkLoadSnapshot after = CreateLoadSnapshot();
        return new BenchmarkLoadObservation(
            Math.Max(0, after.ProcessCpuMilliseconds - before.ProcessCpuMilliseconds),
            after.WorkingSetBytes,
            Math.Max(0, after.AllocatedBytes - before.AllocatedBytes),
            after.Gen0CollectionCount - before.Gen0CollectionCount,
            after.Gen1CollectionCount - before.Gen1CollectionCount,
            after.Gen2CollectionCount - before.Gen2CollectionCount,
            after.ThreadPoolPendingWorkItemCount,
            Math.Max(
                0,
                after.ThreadPoolCompletedWorkItemCount -
                    before.ThreadPoolCompletedWorkItemCount));
    }
}
