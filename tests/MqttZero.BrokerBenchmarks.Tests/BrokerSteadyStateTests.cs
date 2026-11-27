using MqttZero.BrokerBenchmarks;
using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Sessions;
using System.Diagnostics;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BrokerSteadyStateTests
{
    private const int PausedPayloadSize = 8 * 1024;

    [Fact(Timeout = 60_000)]
    public async Task PausedSubscriberKeepsBrokerWithinItsBufferBudget()
    {
        const long BufferBudget = 2 * 1024 * 1024;
        await using TestBrokerHost host = await TestBrokerHost.StartAsync(
            options =>
            {
                options.MaxMessageQueueLength = 8;
                options.MaxBufferedBytes = BufferBudget;
            });
        await using RawMqttClient subscriber = host.CreateRawClient();
        await using RawMqttClient publisher = host.CreateRawClient();
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        await subscriber.ConnectAsync("paused-subscriber", cancellationToken);
        await subscriber.SubscribeAsync(
            "steady/paced",
            MqttQualityOfServiceLevel.AtLeastOnce,
            cancellationToken);
        await publisher.ConnectAsync("paced-publisher", cancellationToken);

        byte[] payload = new byte[PausedPayloadSize];
        subscriber.PauseReading();
        await publisher.PublishAsync(
            "steady/paced",
            payload,
            MqttQualityOfServiceLevel.AtLeastOnce,
            cancellationToken);
        await subscriber.WaitForPauseAsync(cancellationToken);

        for (int index = 0; index < 24; index++)
        {
            await publisher.PublishAsync(
                "steady/paced",
                payload,
                MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken);
            MqttZeroBrokerMemorySnapshot snapshot =
                host.Broker.GetMemorySnapshotForTest();
            Assert.InRange(
                snapshot.BufferCommittedBytes,
                0,
                BufferBudget);
            Assert.InRange(snapshot.BufferRentedBytes, 0, BufferBudget);
        }

        subscriber.ResumeReading();
        await TestBrokerHost.WaitUntilAsync(
            () => subscriber.ReceivedMessageCount > 0,
            TimeSpan.FromSeconds(10),
            cancellationToken);

        MqttZeroBrokerMemorySnapshot drained =
            host.Broker.GetMemorySnapshotForTest();
        Assert.Equal(0, drained.RoutePlanInUseCount);
    }

    [Fact(Timeout = 60_000)]
    public async Task SubscriptionChurnReturnsRouteRetentionToBaseline()
    {
        await using TestBrokerHost host = await TestBrokerHost.StartAsync();
        await using RawMqttClient client = host.CreateRawClient();
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        await client.ConnectAsync("churn-client", cancellationToken);
        MqttZeroBrokerMemorySnapshot baseline =
            host.Broker.GetMemorySnapshotForTest();

        for (int round = 0; round < 20; round++)
        {
            string filter = $"churn/{round}/#";
            await client.SubscribeAsync(
                filter,
                MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken);
            await client.UnsubscribeAsync(filter, cancellationToken);
        }

        MqttZeroBrokerMemorySnapshot churned =
            host.Broker.GetMemorySnapshotForTest();
        Assert.Equal(0, churned.RoutePlanInUseCount);
        Assert.Equal(0, churned.SharedCursorCount);
        Assert.InRange(
            churned.RoutePlanRetainedBytes,
            0,
            baseline.RoutePlanRetainedBytes + (1024 * 1024));
    }

    [Fact(Timeout = 120_000)]
    public async Task StopDrainsPendingEgressAndReleasesBuffers()
    {
        TestBrokerHost host = await TestBrokerHost.StartAsync(
            options =>
            {
                options.MaxMessageQueueLength = 64;
            });
        try
        {
            await using RawMqttClient subscriber = host.CreateRawClient();
            await using RawMqttClient publisher = host.CreateRawClient();
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            await subscriber.ConnectAsync(
                "stop-subscriber",
                cancellationToken);
            await subscriber.SubscribeAsync(
                "steady/stop",
                MqttQualityOfServiceLevel.AtMostOnce,
                cancellationToken);
            await publisher.ConnectAsync(
                "stop-publisher",
                cancellationToken);
            subscriber.PauseReading();

            byte[] payload = new byte[PausePayloadSize];
            await publisher.PublishAsync(
                "steady/stop",
                payload,
                MqttQualityOfServiceLevel.AtMostOnce,
                cancellationToken);
            await subscriber.WaitForPauseAsync(cancellationToken);
            for (int index = 0; index < 32; index++)
            {
                await publisher.PublishAsync(
                    "steady/stop",
                    payload,
                    MqttQualityOfServiceLevel.AtMostOnce,
                    cancellationToken);
            }

            await host.Broker.StopAsync(cancellationToken)
                .AsTask()
                .WaitAsync(
                    TimeSpan.FromSeconds(40),
                    cancellationToken);
            // 最后一次 flush 的续接可能在 StopAsync 返回后才回到传输线程，
            // 因此给释放留一个有限的收敛窗口；超时仍然视为泄漏。
            MqttZeroBrokerMemorySnapshot stopped =
                host.Broker.GetMemorySnapshotForTest();
            long deadline =
                Stopwatch.GetTimestamp() + (Stopwatch.Frequency * 5);
            while (stopped.BufferRentedBytes != 0 &&
                Stopwatch.GetTimestamp() < deadline)
            {
                await Task.Delay(20, cancellationToken);
                stopped = host.Broker.GetMemorySnapshotForTest();
            }

            Assert.True(
                stopped.BufferRentedBytes == 0,
                $"BufferRentedBytes={stopped.BufferRentedBytes}, "
                + $"leasePool={stopped.ObjectPools.LeaseCount}, "
                + $"queuedMessagePool={stopped.ObjectPools.QueuedMessageCount}, "
                + $"inboundWorkPool={stopped.ObjectPools.InboundWorkCount}, "
                + $"inboundShardWorkPool={stopped.ObjectPools.InboundShardWorkCount}, "
                + $"fanoutOperationPool={stopped.ObjectPools.FanoutOperationCount}, "
                + $"fanoutBatchPool={stopped.ObjectPools.FanoutBatchWorkCount}, "
                + $"fanoutScheduledPool={stopped.ObjectPools.FanoutScheduledBatchCount}, "
                + $"routePlanInUse={stopped.RoutePlanInUseCount}, "
                + $"sharedCursors={stopped.SharedCursorCount}.");
            Assert.Equal(0, stopped.RoutePlanInUseCount);
        }
        finally
        {
            await host.DisposeAsync();
        }
    }

    private const int PausePayloadSize = 16 * 1024;
}

