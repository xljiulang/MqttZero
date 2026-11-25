using MqttZero.BrokerBenchmarks;
using System.Runtime;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkRuntimeConfigurationTests
{
    [Fact]
    public void BrokerEnvironmentVariablesUseExplicitRuntimeConfiguration()
    {
        BenchmarkRuntimeConfiguration configuration = new(8);

        IReadOnlyDictionary<string, string> environment =
            configuration.CreateBrokerEnvironmentVariables();

        Assert.Equal("1", environment["DOTNET_gcServer"]);
        Assert.Equal("1", environment["DOTNET_gcConcurrent"]);
        Assert.Equal("1", environment["DOTNET_TieredPGO"]);
        Assert.Equal("8", environment["DOTNET_ThreadPool_ForceMinWorkerThreads"]);
    }

    [Fact]
    public void RejectsNonPositiveMinimumWorkerThreads()
    {
        Assert.Throws<ArgumentOutOfRangeException>(
            () => new BenchmarkRuntimeConfiguration(0));
    }

    [Fact]
    public void ApplyToCurrentProcessDoesNotLowerExistingMinimumWorkers()
    {
        ThreadPool.GetMinThreads(out int existingWorkerCount, out int existingIoCount);
        BenchmarkRuntimeConfiguration configuration = new(
            Math.Max(1, existingWorkerCount - 1));

        BenchmarkRuntimeSnapshot snapshot = configuration.ApplyToCurrentProcess();

        Assert.True(snapshot.MinimumWorkerThreads >= existingWorkerCount);
        Assert.Equal(existingIoCount, snapshot.MinimumCompletionPortThreads);
        Assert.Equal(GCSettings.IsServerGC, snapshot.IsServerGarbageCollection);
    }
}
