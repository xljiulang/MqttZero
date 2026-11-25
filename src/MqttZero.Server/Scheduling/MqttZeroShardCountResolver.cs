namespace MqttZero.Server.Scheduling;

internal static class MqttZeroShardCountResolver
{
    private const int MaximumShardCount = 256;

    public static int Resolve(int configuredCount)
    {
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

        return Math.Clamp(
            Environment.ProcessorCount,
            1,
            MaximumShardCount);
    }
}
