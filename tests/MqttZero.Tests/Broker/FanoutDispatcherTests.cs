using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using MqttZero.Tests.Support;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Reflection;
using System.Text;

namespace MqttZero.Tests.Broker;

public sealed class FanoutDispatcherTests
{
    [Fact(Timeout = 10_000)]
    public async Task OnePublishEnqueuesAtMostOneRemoteWorkPerShard()
    {
        const int shardCount = 8;
        await using MqttZeroShardSet shards = new(shardCount, 4096);
        using MqttZeroRoutePlannerLanes lanes = new(shardCount);
        MqttZeroRouteEpoch epoch = CreateFanoutEpoch(
            1_000,
            shardCount);
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        MqttZeroMessageLease lease = CreateLease(bufferPool);
        int delivered = 0;
        MqttZeroFanoutDispatcher dispatcher = new(
            shards,
            (_, _, _, _) =>
            {
                Interlocked.Increment(ref delivered);
                return ValueTask.CompletedTask;
            },
            _ => { });
        long enqueuedBefore = GetEnqueuedCount(shards);

        try
        {
            await shards[0].InvokeAsync(
                async cancellationToken =>
                {
                    MqttZeroRoutePlanBuffer buffer = lanes[0];
                    MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                        epoch,
                        "fanout/topic"u8,
                        null,
                        new MqttZeroSharedGroupCursors(),
                        shardCount,
                        buffer);
                    await dispatcher.DispatchAsync(
                        plan,
                        buffer,
                        lease,
                        null,
                        0,
                        cancellationToken);
                    return true;
                },
                TestContext.Current.CancellationToken).AsTask()
                .WaitAsync(TestContext.Current.CancellationToken);

            long enqueuedDelta =
                GetEnqueuedCount(shards) - enqueuedBefore - 1;
            Assert.InRange(enqueuedDelta, 0, shardCount - 1);
            Assert.Equal(1_000, delivered);
        }
        finally
        {
            lease.Release();
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task PublishesFromOneShardPreservePerSessionOrder()
    {
        const int shardCount = 4;
        await using MqttZeroShardSet shards = new(shardCount, 256);
        using MqttZeroRoutePlannerLanes lanes = new(shardCount);
        MqttZeroSessionState second = new("second", 100, 1);
        MqttZeroSessionState third = new("third", 100, 2);
        MqttZeroSessionState fourth = new("fourth", 100, 3);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty
            .Add(second, CreateSubscription("order/#"))
            .Add(third, CreateSubscription("order/#"))
            .Add(fourth, CreateSubscription("order/#"));
        Dictionary<string, List<byte>> received =
            new(StringComparer.Ordinal);
        object receivedGate = new();
        MqttZeroFanoutDispatcher dispatcher = new(
            shards,
            (delivery, lease, _, _) =>
            {
                lock (receivedGate)
                {
                    if (!received.TryGetValue(
                        delivery.Session.ClientId,
                        out List<byte>? payloads))
                    {
                        payloads = [];
                        received[delivery.Session.ClientId] = payloads;
                    }

                    payloads.Add(lease.Payload.Span[0]);
                }

                return ValueTask.CompletedTask;
            },
            _ => { });

        await shards[0].InvokeAsync(
            async cancellationToken =>
            {
                MqttZeroRoutePlanBuffer buffer = lanes[0];
                for (byte payload = 0; payload < 3; payload++)
                {
                    MqttZeroMessageLease lease = CreateLease(
                        payload: [payload]);
                    try
                    {
                        MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                            epoch,
                            "order/topic"u8,
                            null,
                            new MqttZeroSharedGroupCursors(),
                            shardCount,
                            buffer);
                        await dispatcher.DispatchAsync(
                            plan,
                            buffer,
                            lease,
                            null,
                            0,
                            cancellationToken);
                    }
                    finally
                    {
                        lease.Release();
                    }
                }

                return true;
            },
            TestContext.Current.CancellationToken).AsTask()
            .WaitAsync(TestContext.Current.CancellationToken);

        Assert.Equal([0, 1, 2], received["second"]);
        Assert.Equal([0, 1, 2], received["third"]);
        Assert.Equal([0, 1, 2], received["fourth"]);
    }

    [Fact(Timeout = 10_000)]
    public async Task LeaseRemainsAliveUntilRemoteBatchCompletes()
    {
        const int shardCount = 2;
        await using MqttZeroShardSet shards = new(shardCount, 16);
        using MqttZeroRoutePlannerLanes lanes = new(shardCount);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty.Add(
            new MqttZeroSessionState("receiver", 100, 1),
            CreateSubscription("lease/#"));
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        MqttZeroMessageLease lease = CreateLease(bufferPool);
        TaskCompletionSource deliveryStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource deliveryRelease = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        MqttZeroFanoutDispatcher dispatcher = new(
            shards,
            async (_, _, _, _) =>
            {
                deliveryStarted.TrySetResult();
                await deliveryRelease.Task.ConfigureAwait(false);
            },
            _ => { });
        bool callerLeaseReleased = false;
        Task dispatch = Task.CompletedTask;

        try
        {
            dispatch = shards[0].InvokeAsync(
                async cancellationToken =>
                {
                    MqttZeroRoutePlanBuffer buffer = lanes[0];
                    MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                        epoch,
                        "lease/topic"u8,
                        null,
                        new MqttZeroSharedGroupCursors(),
                        shardCount,
                        buffer);
                    await dispatcher.DispatchAsync(
                        plan,
                        buffer,
                        lease,
                        null,
                        0,
                        cancellationToken);
                    return true;
                },
                TestContext.Current.CancellationToken).AsTask();

            await deliveryStarted.Task.WaitAsync(
                TestContext.Current.CancellationToken);
            lease.Release();
            callerLeaseReleased = true;
            Assert.True(bufferPool.RentedBytes > 0);

            deliveryRelease.TrySetResult();
            await dispatch.WaitAsync(TestContext.Current.CancellationToken);
            Assert.Equal(0, bufferPool.RentedBytes);
        }
        finally
        {
            deliveryRelease.TrySetResult();
            if (!callerLeaseReleased)
            {
                lease.Release();
            }

            if (!dispatch.IsCompleted)
            {
                try
                {
                    await dispatch.WaitAsync(
                        TimeSpan.FromSeconds(2),
                        CancellationToken.None);
                }
                catch (Exception)
                {
                }
            }
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task LeaseIsReleasedWhenRemoteBatchFaults()
    {
        const int shardCount = 2;
        await using MqttZeroShardSet shards = new(shardCount, 16);
        using MqttZeroRoutePlannerLanes lanes = new(shardCount);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty.Add(
            new MqttZeroSessionState("receiver", 100, 1),
            CreateSubscription("lease/#"));
        using MqttZeroBufferPool bufferPool = new(1024 * 1024);
        MqttZeroMessageLease lease = CreateLease(bufferPool);
        InvalidOperationException expected = new("remote failure");
        MqttZeroFanoutDispatcher dispatcher = new(
            shards,
            (_, _, _, _) => throw expected,
            _ => { });
        bool leaseReleased = false;

        try
        {
            InvalidOperationException actual =
                await Assert.ThrowsAsync<InvalidOperationException>(() =>
                    shards[0].InvokeAsync(
                        async cancellationToken =>
                        {
                            MqttZeroRoutePlanBuffer buffer = lanes[0];
                            MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
                                epoch,
                                "lease/topic"u8,
                                null,
                                new MqttZeroSharedGroupCursors(),
                                shardCount,
                                buffer);
                            await dispatcher.DispatchAsync(
                                plan,
                                buffer,
                                lease,
                                null,
                                0,
                                cancellationToken);
                            return true;
                        },
                        TestContext.Current.CancellationToken).AsTask());

            Assert.Same(expected, actual);
            lease.Release();
            leaseReleased = true;
            Assert.Equal(0, bufferPool.RentedBytes);
        }
        finally
        {
            if (!leaseReleased)
            {
                lease.Release();
            }
        }
    }

    [Fact(Timeout = 10_000)]
    public async Task BrokerQueuesIdentifiersIndependentlyOfReusedPlanBuffer()
    {
        MqttZeroOptions options = new()
        {
            MaxMessageQueueLength = 10,
            StoreQos0InQueue = true
        };
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            options,
            TimeProvider.System);
        await broker.StartAsync(TestContext.Current.CancellationToken);
        MqttZeroSessionState firstReceiver =
            broker.CreateSessionState("first-receiver");
        firstReceiver.Subscriptions.Add(CreateSubscription(
            "first/#",
            identifier: 7));
        firstReceiver.Subscriptions.Add(CreateSubscription(
            "first/+",
            identifier: 9));
        MqttZeroSessionState secondReceiver =
            broker.CreateSessionState("second-receiver");
        secondReceiver.Subscriptions.Add(CreateSubscription(
            "second/#",
            identifier: 11));
        secondReceiver.Subscriptions.Add(CreateSubscription(
            "second/+",
            identifier: 22));
        AddSession(broker, firstReceiver);
        AddSession(broker, secondReceiver);
        broker.ResetRouteEpochFromSessions();
        MqttZeroSessionState publisher =
            broker.CreateSessionState("publisher");
        PipeWriter writer = PipeWriter.Create(Stream.Null);
        await using MqttZeroClientConnection publisherConnection = new(
            writer,
            new IPEndPoint(IPAddress.Loopback, 0),
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);

        await RouteQosZeroAsync(
            broker,
            publisher,
            publisherConnection,
            "first/topic");
        await RouteQosZeroAsync(
            broker,
            publisher,
            publisherConnection,
            "second/topic");
        await WaitForPendingMessageAsync(
            firstReceiver,
            TestContext.Current.CancellationToken);
        await WaitForPendingMessageAsync(
            secondReceiver,
            TestContext.Current.CancellationToken);

        lock (firstReceiver.SyncRoot)
        {
            MqttZeroQueuedMessage queued =
                firstReceiver.PendingMessages.Dequeue();
            try
            {
                Assert.Equal(
                    [7U, 9U],
                    queued.SubscriptionIdentifiers.ToArray());
            }
            finally
            {
                queued.Release();
            }
        }
    }

    [Fact]
    public async Task LocalDispatchDoesNotAllocateAfterWarmup()
    {
        await using MqttZeroShardSet shards = new(1, 8);
        using MqttZeroRoutePlannerLanes lanes = new(1);
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty.Add(
            new MqttZeroSessionState("receiver", 100, 0),
            CreateSubscription("local/#"));
        MqttZeroRoutePlanBuffer buffer = lanes[0];
        MqttZeroMessageLease lease = CreateLease();
        MqttZeroFanoutDispatcher dispatcher = new(
            shards,
            (_, _, _, _) => ValueTask.CompletedTask,
            _ => { });
        MqttZeroRoutePlan plan = MqttZeroRoutePlanner.Plan(
            epoch,
            "local/topic"u8,
            null,
            new MqttZeroSharedGroupCursors(),
            1,
            buffer);
        try
        {
            for (int index = 0; index < 10; index++)
            {
                await dispatcher.DispatchAsync(
                    plan,
                    buffer,
                    lease,
                    null,
                    0,
                    TestContext.Current.CancellationToken);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1_000; index++)
            {
                await dispatcher.DispatchAsync(
                    plan,
                    buffer,
                    lease,
                    null,
                    0,
                    TestContext.Current.CancellationToken);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(0, allocated);
        }
        finally
        {
            lease.Release();
        }
    }

    [Fact(Timeout = 20_000)]
    public async Task StartedBrokerFanoutDoesNotAllocateAfterWarmup()
    {
        MqttZeroOptions options = new()
        {
            EventLoopShardCount = 1,
            MaxMessageQueueLength = 4,
            StoreQos0InQueue = true
        };
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            options,
            TimeProvider.System);
        await broker.StartAsync(TestContext.Current.CancellationToken);
        MqttZeroSessionState receiver =
            broker.CreateSessionState("allocation-receiver");
        receiver.Subscriptions.Add(CreateSubscription("allocation/#"));
        AddSession(broker, receiver);
        broker.ResetRouteEpochFromSessions();
        MqttZeroSessionState sender =
            broker.CreateSessionState("allocation-sender");
        PipeWriter writer = PipeWriter.Create(Stream.Null);
        await using MqttZeroClientConnection senderConnection = new(
            writer,
            new IPEndPoint(IPAddress.Loopback, 0),
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>("allocation/topic"u8.ToArray()),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>([1]),
            MqttQualityOfServiceLevel.AtMostOnce,
            false,
            null,
            null);

        try
        {
            for (int index = 0; index < 10; index++)
            {
                await RouteAndReleaseReceiverMessageAsync(
                    broker,
                    sender,
                    senderConnection,
                    receiver,
                    lease);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1_000; index++)
            {
                await RouteAndReleaseReceiverMessageAsync(
                    broker,
                    sender,
                    senderConnection,
                    receiver,
                    lease);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(0, allocated);
        }
        finally
        {
            lease.Release();
        }
    }

    [Fact(Timeout = 20_000)]
    public async Task ConcurrentShardFanoutOwnsBorrowedLeaseBeforeEncoding()
    {
        MqttZeroOptions options = new()
        {
            EventLoopShardCount = 2,
            MaxMessageQueueLength = 16
        };
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            options,
            TimeProvider.System);
        await broker.StartAsync(TestContext.Current.CancellationToken);
        MqttZeroSessionState? first = null;
        MqttZeroSessionState? second = null;
        for (int index = 0; index < 100; index++)
        {
            MqttZeroSessionState session =
                broker.CreateSessionState($"concurrent-{index}");
            if (first == null && session.ShardId == 0)
            {
                first = session;
            }
            else if (second == null && session.ShardId == 1)
            {
                second = session;
            }

            if (first != null && second != null)
            {
                break;
            }
        }

        Assert.NotNull(first);
        Assert.NotNull(second);
        PipeWriter firstWriter = PipeWriter.Create(Stream.Null);
        PipeWriter secondWriter = PipeWriter.Create(Stream.Null);
        await using MqttZeroClientConnection firstConnection = new(
            firstWriter,
            new IPEndPoint(IPAddress.Loopback, 0),
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        await using MqttZeroClientConnection secondConnection = new(
            secondWriter,
            new IPEndPoint(IPAddress.Loopback, 0),
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        first.Subscriptions.Add(CreateSubscription("concurrent/#"));
        second.Subscriptions.Add(CreateSubscription("concurrent/#"));
        first.Connection = firstConnection;
        second.Connection = secondConnection;
        AddSession(broker, first);
        AddSession(broker, second);
        broker.ResetRouteEpochFromSessions();
        SequenceSegment start = new(new byte[32]);
        SequenceSegment end = new(new byte[32]);
        start.SetNext(end);
        ReadOnlySequence<byte> payload = new(
            start,
            0,
            end,
            end.Memory.Length);

        for (int index = 0; index < 100; index++)
        {
            await broker.PublishAsync(
                "concurrent/topic",
                payload,
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                TestContext.Current.CancellationToken);
            lock (first.SyncRoot)
            {
                while (first.PendingMessages.Count > 0)
                {
                    first.PendingMessages.Dequeue().Release();
                }
            }

            lock (second.SyncRoot)
            {
                while (second.PendingMessages.Count > 0)
                {
                    second.PendingMessages.Dequeue().Release();
                }
            }
        }
    }

    private static MqttZeroRouteEpoch CreateFanoutEpoch(
        int sessions,
        int shardCount)
    {
        MqttZeroRouteEpoch epoch = MqttZeroRouteEpoch.Empty;
        for (int index = 0; index < sessions; index++)
        {
            epoch = epoch.Add(
                new MqttZeroSessionState(
                    $"fanout-{index}",
                    100,
                    index % shardCount),
                CreateSubscription("fanout/#"));
        }

        return epoch;
    }

    private static MqttZeroSubscription CreateSubscription(string filter)
    {
        return CreateSubscription(filter, null);
    }

    private static MqttZeroSubscription CreateSubscription(
        string filter,
        uint? identifier)
    {
        return new MqttZeroSubscription(
            Encoding.UTF8.GetBytes(filter),
            MqttQualityOfServiceLevel.AtMostOnce,
            false,
            false,
            identifier);
    }

    private static MqttZeroMessageLease CreateLease(
        MqttZeroBufferPool? bufferPool = null,
        byte[]? payload = null)
    {
        return MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>("fanout/topic"u8.ToArray()),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>(payload ?? [1]),
            MqttQualityOfServiceLevel.AtMostOnce,
            false,
            null,
            null,
            bufferPool);
    }

    private static long GetEnqueuedCount(MqttZeroShardSet shards)
    {
        long count = 0;
        MqttZeroShardSetSnapshot snapshot = shards.GetSnapshot();
        for (int index = 0; index < snapshot.Shards.Count; index++)
        {
            count += snapshot.Shards[index].EnqueuedCount;
        }

        return count;
    }

    private static async ValueTask RouteQosZeroAsync(
        MqttZeroBroker broker,
        MqttZeroSessionState publisher,
        MqttZeroClientConnection publisherConnection,
        string topic)
    {
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(topic)),
            ReadOnlySequence<byte>.Empty,
            new ReadOnlySequence<byte>([1]),
            MqttQualityOfServiceLevel.AtMostOnce,
            false,
            null,
            null);
        try
        {
            await broker.RoutePublishLeaseAsync(
                publisher,
                publisherConnection,
                lease,
                TestContext.Current.CancellationToken);
        }
        finally
        {
            lease.Release();
        }
    }

    private static async Task WaitForPendingMessageAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout =
            CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        while (true)
        {
            lock (session.SyncRoot)
            {
                if (session.PendingMessages.Count > 0)
                {
                    return;
                }
            }

            await Task.Delay(
                TimeSpan.FromMilliseconds(1),
                timeout.Token);
        }
    }

    private static async ValueTask RouteAndReleaseReceiverMessageAsync(
        MqttZeroBroker broker,
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroSessionState receiver,
        MqttZeroMessageLease lease)
    {
        await broker.RoutePublishLeaseAsync(
            sender,
            senderConnection,
            lease,
            TestContext.Current.CancellationToken);
        lock (receiver.SyncRoot)
        {
            MqttZeroQueuedMessage message =
                receiver.PendingMessages.Dequeue();
            message.Release();
        }
    }

    private static void AddSession(
        MqttZeroBroker broker,
        MqttZeroSessionState session)
    {
        FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
            "_sessions",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        ConcurrentDictionary<string, MqttZeroSessionState> sessions =
            (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField
                .GetValue(broker)!;
        sessions[session.ClientId] = session;
    }
}
