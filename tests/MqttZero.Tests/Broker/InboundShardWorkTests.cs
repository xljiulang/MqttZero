using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Tests.Broker;

public sealed class InboundShardWorkTests
{
    [Fact(Timeout = 5_000)]
    public async Task WorkFromOneConnectionRunsInOrderAndReleasesLeases()
    {
        await using MqttZeroShardSet shards = new(2, 8);
        MqttZeroEventLoop shard = shards[0];
        List<int> order = [];
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        for (int index = 1; index <= 3; index++)
        {
            MqttZeroInboundWork inner = CreateWork(index);
            MqttZeroInboundShardWork work = MqttZeroInboundShardWork.Rent(
                inner,
                (current, _) =>
                {
                    order.Add(current.Lease.PacketIdentifier);
                    if (order.Count == 3)
                    {
                        completed.TrySetResult();
                    }

                    return ValueTask.CompletedTask;
                },
                _ => { });
            await shard.EnqueueAsync(
                work,
                TestContext.Current.CancellationToken);
        }

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shards.StopAsync();

        Assert.Equal([1, 2, 3], order);
        Assert.Equal(3, shard.GetSnapshot().CompletedCount);
        Assert.Equal(0, shard.GetSnapshot().FailedCount);
    }

    [Fact(Timeout = 5_000)]
    public async Task FailureInvokesCallbackAndReleasesWork()
    {
        await using MqttZeroShardSet shards = new(1, 8);
        MqttZeroEventLoop shard = shards[0];
        InvalidOperationException expected = new("failed");
        TaskCompletionSource<Exception> faulted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        MqttZeroInboundShardWork work = MqttZeroInboundShardWork.Rent(
            CreateWork(1),
            (_, _) => throw expected,
            exception => faulted.TrySetResult(exception));

        await shard.EnqueueAsync(
            work,
            TestContext.Current.CancellationToken);

        Assert.Same(
            expected,
            await faulted.Task.WaitAsync(
                TestContext.Current.CancellationToken));
        await shards.StopAsync();
        Assert.Equal(1, shard.GetSnapshot().FailedCount);
    }

    [Fact]
    public void InboundWorkOwnsIndependentLeaseReference()
    {
        MqttZeroBufferPool pool = new(1024 * 1024);
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>("lease/topic"u8.ToArray()),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>(new byte[1024]),
            MqttQualityOfServiceLevel.AtLeastOnce,
            false,
            null,
            null,
            pool);
        MqttZeroInboundWork work = MqttZeroInboundWork.Rent(
            lease,
            new MqttZeroSessionState("sender"),
            null);

        work.Release();

        Assert.True(pool.RentedBytes > 0);
        lease.Release();
        Assert.Equal(0, pool.RentedBytes);
    }

    private static MqttZeroInboundWork CreateWork(int packetIdentifier)
    {
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>("order/topic"u8.ToArray()),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>(new byte[8]),
            MqttQualityOfServiceLevel.AtLeastOnce,
            false,
            null,
            null);
        lease.PacketIdentifier = (ushort)packetIdentifier;
        return MqttZeroInboundWork.Rent(
            lease,
            new MqttZeroSessionState("sender"),
            null);
    }
}
