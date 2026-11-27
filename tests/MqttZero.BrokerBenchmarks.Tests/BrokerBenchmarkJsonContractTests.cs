using System.Text.Json;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BrokerBenchmarkJsonContractTests
{
    [Fact]
    public void ResultJsonSeparatesAcceptanceAndDeliveryMetrics()
    {
        BrokerBenchmarkResult result = new()
        {
            LoadObservation = new BenchmarkLoadObservation(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0)
        };

        string json = JsonSerializer.Serialize(
            result,
            BrokerBenchmarkJsonContext.Default.BrokerBenchmarkResult);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;

        Assert.True(
            root.TryGetProperty(
                "EndToEndThroughputPerSecond",
                out _));
        Assert.True(
            root.TryGetProperty(
                "PublishCallLatencyP99Microseconds",
                out _));
        Assert.True(
            root.TryGetProperty(
                "MeasurementReceivedMessages",
                out _));
        Assert.True(
            root.TryGetProperty(
                "MeasurementDrainMilliseconds",
                out _));
    }

    [Fact]
    public void OutboundJsonIncludesFinalDrainContract()
    {
        OutboundBenchmarkResult result = new(
            PublishedMessages: 1,
            ReceivedMessages: 1,
            ExpectedReceivedMessages: 1,
            FailedOperations: 0,
            ThroughputPerSecond: 1,
            EndToEndThroughputPerSecond: 1,
            MeasurementDrainMilliseconds: 0,
            WaitP50Microseconds: 0,
            WaitP99Microseconds: 0,
            PublishP50Microseconds: 0,
            PublishP99Microseconds: 0,
            FirstError: null,
            Diagnostics: null!);

        string json = JsonSerializer.Serialize(
            result,
            BrokerBenchmarkJsonContext.Default.OutboundBenchmarkResult);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;

        Assert.True(
            root.TryGetProperty(
                "ExpectedReceivedMessages",
                out _));
        Assert.True(
            root.TryGetProperty(
                "EndToEndThroughputPerSecond",
                out _));
        Assert.True(
            root.TryGetProperty(
                "MeasurementDrainMilliseconds",
                out _));
    }

    [Fact]
    public void MemoryJsonIncludesBrokerOwnershipAndWaveMetrics()
    {
        MemoryBenchmarkResult result = new(
            [
                new MemoryWaveResult(
                    Wave: 1,
                    PublishedMessages: 10,
                    ReceivedMessages: 1000,
                    FailedOperations: 0,
                    MeasurementDrainMilliseconds: 1,
                    Snapshot: new BenchmarkMemorySnapshot(
                        AllocatedBytes: 1,
                        ManagedHeapBytes: 1,
                        WorkingSetBytes: 1,
                        PrivateMemoryBytes: 1,
                        Gen0CollectionCount: 0,
                        Gen1CollectionCount: 0,
                        Gen2CollectionCount: 0,
                        BufferRentedBytes: 1,
                        BufferCachedBytes: 1,
                        BufferCommittedBytes: 2,
                        RoutePlanAvailableCount: 1,
                        RoutePlanInUseCount: 0,
                        RoutePlanRetainedBytes: 1,
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
                                AcknowledgementShardWorkCount: 0)))
            ]);

        string json = JsonSerializer.Serialize(
            result,
            BrokerBenchmarkJsonContext.Default.MemoryBenchmarkResult);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;
        JsonElement wave = root.GetProperty("Waves")[0];
        JsonElement snapshot = wave.GetProperty("Snapshot");

        Assert.True(snapshot.TryGetProperty("BufferCommittedBytes", out _));
        Assert.True(snapshot.TryGetProperty("RoutePlanRetainedBytes", out _));
        Assert.True(snapshot.TryGetProperty("ManagedHeapBytes", out _));
        Assert.True(snapshot.TryGetProperty("PrivateMemoryBytes", out _));
        Assert.True(snapshot.TryGetProperty("ObjectPools", out _));
    }
}
