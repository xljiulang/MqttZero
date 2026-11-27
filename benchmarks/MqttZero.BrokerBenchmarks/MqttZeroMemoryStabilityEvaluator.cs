namespace MqttZero.BrokerBenchmarks;

internal static class MqttZeroMemoryStabilityEvaluator
{
    private const long AllowanceBytes = 1024 * 1024;
    private const double AllowanceRatio = 1.05;

    public static bool IsStable(
        IReadOnlyList<MemoryWaveResult> waves,
        long maximumBufferCommittedBytes,
        out string? failure)
    {
        ArgumentNullException.ThrowIfNull(waves);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(
            maximumBufferCommittedBytes);
        if (waves.Count < 3)
        {
            failure = "At least three memory waves are required.";
            return false;
        }

        for (int index = 0; index < waves.Count; index++)
        {
            MemoryWaveResult wave = waves[index];
            if (wave.FailedOperations != 0)
            {
                failure = $"Wave {wave.Wave} reported failed operations.";
                return false;
            }

            if (wave.Snapshot.BufferCommittedBytes >
                maximumBufferCommittedBytes)
            {
                failure =
                    $"Wave {wave.Wave} exceeded the committed buffer budget.";
                return false;
            }
        }

        int baselineIndex = Math.Min(2, waves.Count - 1);
        MemoryWaveResult baseline = waves[baselineIndex];
        MemoryWaveResult last = waves[^1];
        if (ExceedsAllowance(
            last.Snapshot.ManagedHeapBytes,
            baseline.Snapshot.ManagedHeapBytes))
        {
            failure = "The final managed heap exceeded the wave allowance.";
            return false;
        }

        if (ExceedsAllowance(
            last.Snapshot.RoutePlanRetainedBytes,
            baseline.Snapshot.RoutePlanRetainedBytes))
        {
            failure =
                "The final route plan retention exceeded the wave allowance.";
            return false;
        }

        long baselineObjects = CountObjects(baseline.Snapshot.ObjectPools);
        long lastObjects = CountObjects(last.Snapshot.ObjectPools);
        if (lastObjects >
            (baselineObjects * AllowanceRatio) + 4_096)
        {
            failure = "The final object pool retention exceeded its allowance.";
            return false;
        }

        if (waves.Count >= 5 &&
            IsMonotonicallyIncreasing(waves) &&
            last.Snapshot.ManagedHeapBytes -
                waves[0].Snapshot.ManagedHeapBytes > AllowanceBytes)
        {
            failure = "The managed heap grew monotonically across all waves.";
            return false;
        }

        failure = null;
        return true;
    }

    private static long CountObjects(
        MqttZero.Server.Broker.MqttZeroObjectPoolSnapshot pools)
    {
        return
            pools.LeaseCount +
            pools.QueuedMessageCount +
            pools.PublishContextCount +
            pools.InboundWorkCount +
            pools.InboundShardWorkCount +
            pools.FanoutOperationCount +
            pools.FanoutBatchWorkCount +
            pools.FanoutScheduledBatchCount +
            pools.AcknowledgementWorkCount +
            pools.AcknowledgementShardWorkCount;
    }

    private static bool ExceedsAllowance(long value, long baseline)
    {
        return value > (baseline * AllowanceRatio) + AllowanceBytes;
    }

    private static bool IsMonotonicallyIncreasing(
        IReadOnlyList<MemoryWaveResult> waves)
    {
        for (int index = 1; index < waves.Count; index++)
        {
            if (waves[index].Snapshot.ManagedHeapBytes <=
                waves[index - 1].Snapshot.ManagedHeapBytes)
            {
                return false;
            }
        }

        return true;
    }
}
