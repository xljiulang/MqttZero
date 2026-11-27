using MqttZero.Memory;
using MqttZero.Server.Broker;
using MqttZero.Server.Egress;
using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Egress;

public sealed class ControlPostWorkPoolTests
{
    [Fact(Timeout = 10_000)]
    public async Task ReleasingUnexecutedWorkFailsAndReturnsTheFrame()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        using MqttZeroEgressControlFramePool framePool =
            new(bufferPool);
        await using MqttZeroSessionEgress egress = CreateEgress(scheduler);
        MqttZeroEgressControlFrame frame = framePool.Rent(4);
        frame.MarkForActorRelease();
        Assert.Equal(0, framePool.RetainedCount);
        MqttZeroControlPostWork work =
            MqttZeroControlPostWork.Rent(egress, frame);

        work.Release();

        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await frame.WaitAsync().AsTask()
                .WaitAsync(cancellationToken));
        Assert.Equal(1, framePool.RetainedCount);
    }

    [Fact(Timeout = 15_000)]
    public async Task ControlPostWorkPoolReachesASteadyState()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        await using MqttZeroEventLoop shard = new(0, 32);
        await using MqttZeroEgressScheduler scheduler = new(
            shard,
            diagnostics: null);
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        using MqttZeroEgressControlFramePool framePool =
            new(bufferPool);
        await using MqttZeroSessionEgress egress = CreateEgress(scheduler);

        for (int iteration = 0; iteration < 1_000; iteration++)
        {
            ReleaseOnePostWork(egress, framePool);
        }

        int steady = MqttZeroControlPostWork.PoolCountForTest;
        for (int iteration = 0; iteration < 1_000; iteration++)
        {
            ReleaseOnePostWork(egress, framePool);
        }

        Assert.Equal(steady, MqttZeroControlPostWork.PoolCountForTest);
        Assert.InRange(
            steady,
            1,
            MqttZeroBoundedObjectPool<
                MqttZeroControlPostWork>.DefaultCapacity);
        await Task.Yield();
    }

    [Fact]
    public async Task BrokerSnapshotReportsTheControlPostPool()
    {
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            new MqttZero.Server.MqttZeroOptions(),
            TimeProvider.System);
        await broker.StartAsync(TestContext.Current.CancellationToken);

        MqttZeroBrokerMemorySnapshot snapshot =
            broker.GetMemorySnapshotForTest();

        Assert.Equal(
            MqttZeroControlPostWork.PoolCountForTest,
            snapshot.ObjectPools.ControlPostWorkCount);
    }

    private static void ReleaseOnePostWork(
        MqttZeroSessionEgress egress,
        MqttZeroEgressControlFramePool framePool)
    {
        MqttZeroEgressControlFrame frame = framePool.Rent(4);
        frame.MarkForActorRelease();
        MqttZeroControlPostWork.Rent(egress, frame).Release();
    }

    private static MqttZeroSessionEgress CreateEgress(
        MqttZeroEgressScheduler scheduler)
    {
        return new MqttZeroSessionEgress(
            scheduler,
            (_, _) => ValueTask.CompletedTask,
            _ => ValueTask.FromResult(false),
            _ => { });
    }
}
