using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Text;

namespace MqttZero.Tests.Broker;

public sealed class QosZeroFanoutPipelineTests
{
    [Fact(Timeout = 10_000)]
    public async Task QosZeroPublishesPipelineWithoutReorderingTargetShard()
    {
        await using MqttZeroShardSet shards = new(2, 16);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty.Add(
            new MqttZeroSessionState("receiver", 100, 1),
            new MqttZeroSubscription(
                Encoding.UTF8.GetBytes("pipeline/#"),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
        using MqttZeroRoutePlanBufferPool buffers = new(2);
        MqttZeroSharedGroupCursors cursors = new();
        TaskCompletionSource firstDeliveryStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirstDelivery = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allDelivered = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        List<byte> deliveredPayloads = [];
        object deliveredGate = new();
        MqttZeroFanoutDispatcher dispatcher = new(
            shards,
            async (_, lease, _, _) =>
            {
                byte payload = lease.Payload.Span[0];
                bool blockFirst = false;
                lock (deliveredGate)
                {
                    deliveredPayloads.Add(payload);
                    if (deliveredPayloads.Count == 1)
                    {
                        blockFirst = true;
                    }

                    if (deliveredPayloads.Count == 3)
                    {
                        allDelivered.TrySetResult();
                    }
                }

                if (blockFirst)
                {
                    firstDeliveryStarted.TrySetResult();
                    await releaseFirstDelivery.Task.ConfigureAwait(false);
                }
            },
            _ => { });
        List<MqttZeroMessageLease> leases = [];

        try
        {
            for (byte payload = 0; payload < 3; payload++)
            {
                MqttZeroRoutePlanBuffer buffer = buffers.Rent();
                MqttZeroMessageLease lease = CreateLease(payload);
                leases.Add(lease);
                MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                    epoch,
                    "pipeline/topic"u8,
                    null,
                    cursors,
                    2,
                    buffer);
                await dispatcher.DispatchQosZeroAsync(
                    plan,
                    buffer,
                    lease,
                    null,
                    0,
                    buffers.ReturnCallback,
                    TestContext.Current.CancellationToken);
                lease.Release();
                leases.Remove(lease);
            }

            await firstDeliveryStarted.Task.WaitAsync(
                TestContext.Current.CancellationToken);
            Assert.Equal(3, buffers.InUseCount);
            releaseFirstDelivery.TrySetResult();
            await allDelivered.Task.WaitAsync(
                TestContext.Current.CancellationToken);
            await WaitUntilAsync(
                () => buffers.AvailableCount == 3,
                TestContext.Current.CancellationToken);
            Assert.Equal([0, 1, 2], deliveredPayloads);
        }
        finally
        {
            releaseFirstDelivery.TrySetResult();
            foreach (MqttZeroMessageLease lease in leases)
            {
                lease.Release();
            }
        }
    }

    [Fact]
    public void BufferPoolReturnsExclusiveBuffers()
    {
        using MqttZeroRoutePlanBufferPool pool = new(4);

        MqttZeroRoutePlanBuffer first = pool.Rent();
        MqttZeroRoutePlanBuffer second = pool.Rent();

        Assert.NotSame(first, second);
        Assert.Equal(2, pool.InUseCount);
        pool.Return(first);
        Assert.Equal(1, pool.InUseCount);
        Assert.Same(first, pool.Rent());
    }

    private static MqttZeroMessageLease CreateLease(byte payload)
    {
        return MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>("pipeline/topic"u8.ToArray()),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>([payload]),
            MqttQualityOfServiceLevel.AtMostOnce,
            false,
            null,
            null);
    }

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(2));
        while (!condition())
        {
            await Task.Delay(
                TimeSpan.FromMilliseconds(1),
                timeout.Token);
        }
    }
}
