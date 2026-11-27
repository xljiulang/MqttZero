namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class MqttZeroMemoryStabilityEvaluatorTests
{
    [Fact]
    public void StableWavesPass()
    {
        MemoryWaveResult[] waves =
        [
            CreateWave(1, managedHeap: 10_000_000),
            CreateWave(2, managedHeap: 10_100_000),
            CreateWave(3, managedHeap: 10_200_000),
            CreateWave(4, managedHeap: 10_250_000),
            CreateWave(5, managedHeap: 10_300_000)
        ];

        bool stable = MqttZeroMemoryStabilityEvaluator.IsStable(
            waves,
            maximumBufferCommittedBytes: 4_194_304,
            out string? failure);

        Assert.True(stable, failure);
    }

    [Fact]
    public void ManagedHeapAboveWaveAllowanceFails()
    {
        MemoryWaveResult[] waves =
        [
            CreateWave(1, managedHeap: 10_000_000),
            CreateWave(2, managedHeap: 10_000_000),
            CreateWave(3, managedHeap: 10_000_000),
            CreateWave(4, managedHeap: 10_000_000),
            CreateWave(5, managedHeap: 20_000_000)
        ];

        bool stable = MqttZeroMemoryStabilityEvaluator.IsStable(
            waves,
            maximumBufferCommittedBytes: 4_194_304,
            out string? failure);

        Assert.False(stable);
        Assert.Contains("managed heap", failure);
    }

    [Fact]
    public void CommittedBufferBytesAboveConfigurationFail()
    {
        MemoryWaveResult[] waves =
        [
            CreateWave(
                1,
                managedHeap: 10_000_000,
                committedBytes: 4_194_305),
            CreateWave(2, managedHeap: 10_000_000),
            CreateWave(3, managedHeap: 10_000_000)
        ];

        bool stable = MqttZeroMemoryStabilityEvaluator.IsStable(
            waves,
            maximumBufferCommittedBytes: 4_194_304,
            out string? failure);

        Assert.False(stable);
        Assert.Contains("committed", failure);
    }

    private static MemoryWaveResult CreateWave(
        int wave,
        long managedHeap,
        long committedBytes = 0)
    {
        return new MemoryWaveResult(
            wave,
            PublishedMessages: 1,
            ReceivedMessages: 1,
            FailedOperations: 0,
            MeasurementDrainMilliseconds: 0,
            new BenchmarkMemorySnapshot(
                AllocatedBytes: 0,
                ManagedHeapBytes: managedHeap,
                WorkingSetBytes: 0,
                PrivateMemoryBytes: 0,
                Gen0CollectionCount: 0,
                Gen1CollectionCount: 0,
                Gen2CollectionCount: 0,
                BufferRentedBytes: 0,
                BufferCachedBytes: committedBytes,
                BufferCommittedBytes: committedBytes,
                RoutePlanAvailableCount: 0,
                RoutePlanInUseCount: 0,
                RoutePlanRetainedBytes: 0,
                SharedCursorCount: 0,
                ObjectPools: new MqttZero.Server.Broker.
                    MqttZeroObjectPoolSnapshot(
                        LeaseCount: 0,
                        QueuedMessageCount: 0,
                        PublishContextCount: 0,
                        InboundWorkCount: 0,
                        InboundShardWorkCount: 0,
                        FanoutOperationCount: 0,
                        FanoutBatchWorkCount: 0,
                        FanoutScheduledBatchCount: 0,
                        AcknowledgementWorkCount: 0,
                        AcknowledgementShardWorkCount: 0)));
    }
}
