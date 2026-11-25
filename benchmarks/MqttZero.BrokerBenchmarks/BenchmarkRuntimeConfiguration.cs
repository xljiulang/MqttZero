using System.Runtime;

namespace MqttZero.BrokerBenchmarks;

internal sealed class BenchmarkRuntimeConfiguration
{
    private const string ConcurrentGarbageCollectionSwitch = "System.GC.Concurrent";
    private const string TieredPgoSwitch = "System.Runtime.TieredPGO";

    public BenchmarkRuntimeConfiguration(int threadPoolMinimumWorkerThreads)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(
            threadPoolMinimumWorkerThreads);
        ThreadPoolMinimumWorkerThreads = threadPoolMinimumWorkerThreads;
    }

    public int ThreadPoolMinimumWorkerThreads { get; }

    public IReadOnlyDictionary<string, string> CreateBrokerEnvironmentVariables()
    {
        return new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["DOTNET_gcServer"] = "1",
            ["DOTNET_gcConcurrent"] = "1",
            ["DOTNET_TieredPGO"] = "1",
            ["DOTNET_ThreadPool_ForceMinWorkerThreads"] =
                ThreadPoolMinimumWorkerThreads.ToString(
                    System.Globalization.CultureInfo.InvariantCulture)
        };
    }

    public BenchmarkRuntimeSnapshot ApplyToCurrentProcess()
    {
        ThreadPool.GetMinThreads(
            out int currentWorkerThreads,
            out int currentCompletionPortThreads);
        ThreadPool.GetMaxThreads(
            out int maximumWorkerThreads,
            out int maximumCompletionPortThreads);

        int effectiveMinimumWorkerThreads = Math.Max(
            currentWorkerThreads,
            ThreadPoolMinimumWorkerThreads);
        if (!ThreadPool.SetMinThreads(
                effectiveMinimumWorkerThreads,
                currentCompletionPortThreads))
        {
            throw new InvalidOperationException(
                "The benchmark minimum ThreadPool worker count could not be applied.");
        }

        ThreadPool.GetMinThreads(
            out effectiveMinimumWorkerThreads,
            out int effectiveMinimumCompletionPortThreads);

        return new BenchmarkRuntimeSnapshot(
            GCSettings.IsServerGC,
            ReadAppContextSwitch(ConcurrentGarbageCollectionSwitch, defaultValue: true),
            ReadAppContextSwitch(TieredPgoSwitch, defaultValue: true),
            effectiveMinimumWorkerThreads,
            maximumWorkerThreads,
            effectiveMinimumCompletionPortThreads,
            maximumCompletionPortThreads);
    }

    private static bool ReadAppContextSwitch(
        string name,
        bool defaultValue)
    {
        return AppContext.TryGetSwitch(name, out bool value)
            ? value
            : defaultValue;
    }
}
