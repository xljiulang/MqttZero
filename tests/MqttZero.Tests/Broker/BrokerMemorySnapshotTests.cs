using MqttZero.Server;
using MqttZero.Server.Broker;

namespace MqttZero.Tests.Broker;

public sealed class BrokerMemorySnapshotTests
{
    [Fact(Timeout = 5_000)]
    public async Task RunningBrokerReportsOwnedMemoryComponents()
    {
        MqttZeroOptions options = new();
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            options,
            TimeProvider.System);
        await broker.StartAsync(TestContext.Current.CancellationToken);

        MqttZeroBrokerMemorySnapshot snapshot =
            broker.GetMemorySnapshotForTest();

        Assert.InRange(
            snapshot.BufferRentedBytes,
            0,
            options.MaxBufferedBytes);
        Assert.InRange(
            snapshot.BufferCachedBytes,
            0,
            options.MaxBufferedBytes);
        Assert.InRange(
            snapshot.BufferCommittedBytes,
            0,
            options.MaxBufferedBytes);
        Assert.Equal(
            snapshot.BufferRentedBytes +
                snapshot.BufferCachedBytes,
            snapshot.BufferCommittedBytes);
        Assert.Equal(0, snapshot.SharedCursorCount);
        Assert.Equal(0, snapshot.RoutePlanRetainedBytes);
        Assert.InRange(
            snapshot.ObjectPools.LeaseCount,
            0,
            MqttZero.Memory.MqttZeroBoundedObjectPool<
                MqttZero.Memory.MqttZeroMessageLease>.DefaultCapacity);
        Assert.InRange(
            snapshot.ObjectPools.QueuedMessageCount,
            0,
            MqttZero.Memory.MqttZeroBoundedObjectPool<
                MqttZero.Server.Sessions.MqttZeroQueuedMessage>.
                DefaultCapacity);
        Assert.InRange(
            snapshot.ObjectPools.AcknowledgementWorkCount,
            0,
            MqttZero.Memory.MqttZeroBoundedObjectPool<
                MqttZero.Server.Broker.MqttZeroAcknowledgementWork>.
                DefaultCapacity);
        Assert.InRange(
            snapshot.ObjectPools.AcknowledgementShardWorkCount,
            0,
            MqttZero.Memory.MqttZeroBoundedObjectPool<
                MqttZero.Server.Broker.MqttZeroAcknowledgementShardWork>.
                DefaultCapacity);
        Assert.InRange(
            snapshot.ObjectPools.ControlPostWorkCount,
            0,
            MqttZero.Memory.MqttZeroBoundedObjectPool<
                MqttZero.Server.Egress.MqttZeroControlPostWork>.
                DefaultCapacity);
    }
}
