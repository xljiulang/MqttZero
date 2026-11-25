namespace MqttZero.Server.Scheduling;

internal static class MqttZeroShardCountResolver
{
    private const int MaximumShardCount = 256;
    private static readonly IMqttZeroProcessorTopologyProbe DefaultProbe =
        new MqttZeroProcessorTopology();

    public static int Resolve(int configuredCount)
    {
        return Resolve(configuredCount, DefaultProbe);
    }

    internal static int Resolve(
        int configuredCount,
        IMqttZeroProcessorTopologyProbe topologyProbe)
    {
        ArgumentNullException.ThrowIfNull(topologyProbe);
        if (configuredCount < 0 || configuredCount > MaximumShardCount)
        {
            throw new ArgumentOutOfRangeException(
                nameof(configuredCount),
                configuredCount,
                $"The event loop shard count must be between 0 and {MaximumShardCount}.");
        }

        if (configuredCount > 0)
        {
            return configuredCount;
        }

        int physicalCoreCount = topologyProbe.GetPhysicalCoreCount();
        int resolvedCount = physicalCoreCount > 0
            ? Math.Min(physicalCoreCount, Environment.ProcessorCount)
            : Environment.ProcessorCount;
        return Math.Clamp(resolvedCount, 1, MaximumShardCount);
    }
}
