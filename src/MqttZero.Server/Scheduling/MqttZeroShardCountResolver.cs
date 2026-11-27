namespace MqttZero.Server.Scheduling;

/// <summary>
/// 解析实际使用的分片数量。
/// </summary>
internal static class MqttZeroShardCountResolver
{
    private const int MaximumShardCount = 256;

    /// <summary>
    /// 解析。
    /// </summary>
    /// <param name="configuredCount">configured数量。</param>
    /// <returns>计算得到的数值。</returns>
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
