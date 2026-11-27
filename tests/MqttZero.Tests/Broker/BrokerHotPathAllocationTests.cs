using MqttZero.Server;
using MqttZero.Tests.Support;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Net;
using System.Reflection;


namespace MqttZero.Tests.Broker
{
    public sealed class BrokerHotPathAllocationTests
    {
        [Fact]
        public async Task OfflineQueuePromotesBorrowedLeaseAndReleasesOwnedCopy()
        {
            MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    MaxMessageQueueLength = 1
                },
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            FieldInfo bufferPoolField = typeof(MqttZeroBroker).GetField(
                "_bufferPool",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            bufferPoolField.SetValue(broker, pool);

            MqttZeroSessionState receiver = new("offline");
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "hot/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null));
            FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
                "_sessions",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            ConcurrentDictionary<string, MqttZeroSessionState> sessions =
                (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField.GetValue(broker)!;
            Assert.True(sessions.TryAdd(receiver.ClientId, receiver));
            await broker.ResetRouteEpochFromSessionsAsync();

            AllocationPipeWriter senderWriter = new(16 * 1024);
            MqttZeroClientConnection senderConnection = new(
                senderWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System,
                pool);
            MqttZeroSessionState sender = new("sender");
            MqttZeroMessageLease borrowed = MqttZeroMessageLease.BorrowFrom(
                new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[1024]),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null,
                pool);
            try
            {
                await broker.RoutePublishLeaseAsync(
                    sender,
                    senderConnection,
                    borrowed,
                    TestContext.Current.CancellationToken);
            }
            finally
            {
                borrowed.Release();
            }

            WaitForPendingMessage(receiver);
            Assert.Equal(1, receiver.PendingMessages.Count);
            Assert.True(pool.RentedBytes > 0);
            await broker.DisposeAsync();
            Assert.Equal(0, pool.RentedBytes);
        }
        [Fact]
        public async Task BrokerPublishWithoutInterceptorDoesNotAllocateAfterWarmup()
        {
            await AssertBrokerPublishDoesNotAllocateAfterWarmup([]);
        }
        [Fact]
        public async Task BrokerPublishWithSynchronousInterceptorDoesNotAllocateAfterWarmup()
        {
            await AssertBrokerPublishDoesNotAllocateAfterWarmup([new NoOpPublishInterceptor()]);
        }
        [Fact]
        public async Task ClientConnectionPublishSendDoesNotAllocateAfterWarmup()
        {
            AllocationPipeWriter receiverWriter = new(16 * 1024);
            MqttZeroClientConnection connection = new(
                receiverWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[1024]),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
            MqttZeroQueuedMessage message = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);

            for (int index = 0; index < 10; index++)
            {
                await connection.SendPublishAsync(
                    message,
                    false,
                    TestContext.Current.CancellationToken);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                await connection.SendPublishAsync(
                    message,
                    false,
                    TestContext.Current.CancellationToken);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            message.Release();
            lease.Release();
            await connection.DisposeAsync();
            Assert.Equal(0, allocated);
        }

        [Fact]
        public async Task ClientConnectionControlPacketsDoNotAllocateAfterWarmup()
        {
            AllocationPipeWriter writer = new(1024);
            MqttZeroClientConnection connection = new(
                writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttConnAckPacket connAck = new()
            {
                SessionPresent = true,
                ReasonCode = MqttReasonCode.Success,
                TopicAliasMaximum = 16,
                ReceiveMaximum = 32,
                MaximumPacketSize = 1024 * 1024,
                SessionExpiryInterval = 60
            };
            MqttDisconnectPacket disconnect = new()
            {
                ReasonCode = MqttReasonCode.ServerShuttingDown,
                SessionExpiryInterval = 60
            };

            for (int index = 0; index < 10; index++)
            {
                await connection.SendConnAckAsync(
                    connAck,
                    TestContext.Current.CancellationToken);
                await connection.SendDisconnectAsync(
                    disconnect,
                    TestContext.Current.CancellationToken);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                await connection.SendConnAckAsync(
                    connAck,
                    TestContext.Current.CancellationToken);
                await connection.SendDisconnectAsync(
                    disconnect,
                    TestContext.Current.CancellationToken);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            await connection.DisposeAsync();
            Assert.Equal(0, allocated);
        }
        [Fact]
        public async Task BrokerQosOneQueueEnqueueDoesNotAllocateAfterWarmup()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    EventLoopShardCount = 2
                },
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            MqttZeroSessionState receiver = new(
                "qos-receiver",
                1000,
                1);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "hot/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null));
            AllocationPipeWriter receiverWriter = new(16 * 1024);
            using MqttZeroBufferPool bufferPool = new(16 * 1024 * 1024);
            MqttZeroClientConnection receiverConnection = new(
                receiverWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System,
                bufferPool,
                shardId: 1);
            receiver.Connection = receiverConnection;
            FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
                "_sessions",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            ConcurrentDictionary<string, MqttZeroSessionState> sessions =
                (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField.GetValue(broker)!;
            Assert.True(sessions.TryAdd(receiver.ClientId, receiver));
            await broker.ResetRouteEpochFromSessionsAsync();
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>(new byte[1024]),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                PacketIdentifier = 7
            };
            ArrayBufferWriter<byte> encodedWriter = new();
            MqttPacketCodec.Encode(packet, encodedWriter);
            ReadOnlySequence<byte> encodedPublish = new(encodedWriter.WrittenMemory);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            if (!MqttPublishPacketCodec.TryDecode(
                encodedPublish,
                out MqttZeroPublishView view,
                out _))
            {
                throw new InvalidOperationException(
                    "The QoS 1 benchmark PUBLISH could not be decoded.");
            }

            MqttZeroMessageLease sharedLease =
                broker.CreateMessageLease(receiverConnection, view);
            sharedLease.EnsureOwned();

            await broker.Shards![1].InvokeAsync(
                token =>
                {
                    for (int index = 0; index < 10; index++)
                    {
                        RouteQosOnePublish(
                            broker,
                            receiver,
                            sharedLease,
                            token);
                    }

                    return ValueTask.FromResult(true);
                },
                cancellationToken);
            long allocated = await broker.Shards[1].InvokeAsync(
                token =>
                {
                    long before =
                        GC.GetAllocatedBytesForCurrentThread();
                    for (int index = 0; index < 1000; index++)
                    {
                        RouteQosOnePublish(
                            broker,
                            receiver,
                            sharedLease,
                            token);
                    }

                    return ValueTask.FromResult(
                        GC.GetAllocatedBytesForCurrentThread() -
                        before);
                },
                cancellationToken);
            Assert.Empty(receiver.InflightMessages);
            sharedLease.Release();
            WaitForPoolIdle(bufferPool);
            await broker.DisposeAsync();
            Assert.Equal(0, allocated);
        }
        private static void RouteQosOnePublish(
            MqttZeroBroker broker,
            MqttZeroSessionState receiver,
            MqttZeroMessageLease lease,
            CancellationToken cancellationToken)
        {
            ValueTask route = broker.DeliverLeaseToSessionForTestAsync(
                receiver,
                lease,
                cancellationToken);
            if (!route.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException(
                    "The test enqueue unexpectedly became asynchronous.");
            }

            route.GetAwaiter().GetResult();
            MqttZeroQueuedMessage queued =
                receiver.PendingMessages.Dequeue();
            queued.Release();
        }

        private static void DrainPendingMessages(
            MqttZeroSessionState receiver,
            int expectedCount)
        {
            long deadline = Stopwatch.GetTimestamp() +
                (Stopwatch.Frequency * 5);
            while (receiver.PendingMessages.Count < expectedCount)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                {
                    throw new TimeoutException(
                        $"Expected {expectedCount} queued messages, " +
                        $"but found {receiver.PendingMessages.Count}.");
                }

                Thread.Yield();
            }

            while (receiver.PendingMessages.Count > 0)
            {
                MqttZeroQueuedMessage queued =
                    receiver.PendingMessages.Dequeue();
                queued.Release();
            }
        }

        private static void WaitForPoolIdle(
            MqttZeroBufferPool bufferPool)
        {
            long deadline = Stopwatch.GetTimestamp() +
                (Stopwatch.Frequency * 5);
            while (bufferPool.RentedBytes != 0)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                {
                    throw new TimeoutException(
                        "The fanout lease did not return to the buffer pool.");
                }

                Thread.Yield();
            }
        }

        private static void WaitForFanoutSchedulerIdle(
            MqttZeroFanoutScheduler fanoutScheduler)
        {
            long deadline = Stopwatch.GetTimestamp() +
                (Stopwatch.Frequency * 5);
            while (!fanoutScheduler.IsIdle)
            {
                if (Stopwatch.GetTimestamp() >= deadline)
                {
                    throw new TimeoutException(
                        "The fanout scheduler did not become idle.");
                }

                Thread.Yield();
            }
        }

        private static void WaitForPendingMessage(
            MqttZeroSessionState receiver)
        {
            long deadline = Stopwatch.GetTimestamp() +
                (Stopwatch.Frequency * 5);
            while (Stopwatch.GetTimestamp() < deadline)
            {
                if (receiver.PendingMessages.Count > 0)
                {
                    return;
                }

                Thread.Yield();
            }

            throw new TimeoutException(
                "The routed MQTT message did not enter the receiver queue.");
        }

        private static async Task AssertBrokerPublishDoesNotAllocateAfterWarmup(
            IMqttZeroPublishInterceptor[] interceptors)
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    MaxMessageQueueLength = 1
                },
                TimeProvider.System,
                interceptors);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
                "_sessions",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            ConcurrentDictionary<string, MqttZeroSessionState> sessions =
                (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField.GetValue(broker)!;
            MqttZeroSessionState receiver = new("receiver");
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "hot/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            AllocationPipeWriter receiverWriter = new(16 * 1024);
            MqttZeroClientConnection receiverConnection = new(
                receiverWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            receiver.Connection = receiverConnection;
            Assert.True(sessions.TryAdd(receiver.ClientId, receiver));
            await broker.ResetRouteEpochFromSessionsAsync();

            MqttZeroSessionState sender = new("sender");
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>(new byte[1024])
            };
            ArrayBufferWriter<byte> encodedWriter = new();
            MqttPacketCodec.Encode(packet, encodedWriter);
            ReadOnlySequence<byte> encodedPublish = new(encodedWriter.WrittenMemory);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;

            long allocated = await broker.Shards![0].InvokeAsync(
                async token =>
                {
                    for (int index = 0; index < 10; index++)
                    {
                        receiverWriter.Reset();
                        await RouteEncodedPublishAsync(
                            broker,
                            sender,
                            receiver,
                            receiverConnection,
                            encodedPublish,
                            token);
                    }

                    long before =
                        GC.GetAllocatedBytesForCurrentThread();
                    for (int index = 0; index < 1000; index++)
                    {
                        receiverWriter.Reset();
                        await RouteEncodedPublishAsync(
                            broker,
                            sender,
                            receiver,
                            receiverConnection,
                            encodedPublish,
                            token);
                    }

                    return GC.GetAllocatedBytesForCurrentThread() -
                        before;
                },
                cancellationToken);
            await broker.DisposeAsync();
            Assert.Equal(0, allocated);
        }

        private static async ValueTask RouteEncodedPublishAsync(
            MqttZeroBroker broker,
            MqttZeroSessionState sender,
            MqttZeroSessionState receiver,
            MqttZeroClientConnection receiverConnection,
            ReadOnlySequence<byte> encodedPublish,
            CancellationToken cancellationToken)
        {
            if (!MqttPublishPacketCodec.TryDecode(
                encodedPublish,
                out MqttZeroPublishView view,
                out _))
            {
                throw new InvalidOperationException("The benchmark PUBLISH could not be decoded.");
            }

            MqttZeroMessageLease lease = broker.CreateMessageLease(receiverConnection, view);
            try
            {
                await broker.RoutePublishLeaseAsync(
                    sender,
                    receiverConnection,
                    lease,
                    cancellationToken);
            }
            finally
            {
                lease.Release();
            }

            if (receiver.PendingMessages.Count == 0)
            {
                throw new InvalidOperationException("The PUBLISH was not queued.");
            }

            MqttZeroQueuedMessage queued =
                receiver.PendingMessages.Dequeue();
            queued.Release();
        }

    }
}
