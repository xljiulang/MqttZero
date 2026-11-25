using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Routing;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Reflection;

namespace MqttZero.Tests.Broker
{
    public sealed class OutboundDeliveryTests
    {
        [Fact(Timeout = 5_000)]
        public async Task QosOnePublisherIsAcknowledgedBeforeSubscriberFlushCompletes()
        {
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 100
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            BlockingPipeWriter subscriberWriter = new();
            MqttZeroClientConnection subscriberConnection = new(
                subscriberWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState receiver = new("receiver", options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "large/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null));
            receiver.Connection = subscriberConnection;
            AddSession(broker, receiver);
            RebuildRoutes(broker);

            using CancellationTokenSource pumpLifetime = new();
            MqttZeroEgressActor pump = broker.StartEgress(
                receiver,
                subscriberConnection,
                pumpLifetime.Token);
            TrackingPipeWriter publisherWriter = new();
            MqttZeroClientConnection publisherConnection = new(
                publisherWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("large/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[64 * 1024]),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            lease.PacketIdentifier = 7;

            try
            {
                ValueTask route = broker.RoutePublishLeaseAsync(
                    publisher,
                    publisherConnection,
                    lease,
                    TestContext.Current.CancellationToken);
                await subscriberWriter.FlushStarted.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);

                Assert.True(route.IsCompletedSuccessfully);
                Assert.True(MqttPacketCodec.TryDecode(
                    publisherWriter.WrittenSequence,
                    out MqttPacket? packet,
                    out _));
                MqttPubAckPacket pubAck = Assert.IsType<MqttPubAckPacket>(packet);
                Assert.Equal((ushort)7, pubAck.PacketIdentifier);
            }
            finally
            {
                subscriberWriter.ReleaseFlush();
                await pump.StopAsync();
                lease.Release();
                await publisherConnection.DisposeAsync();
                await subscriberConnection.DisposeAsync();
            }
        }

        [Fact(Timeout = 10_000)]
        public async Task BrokerStopCancelsSubscriberFlushBlockedPump()
        {
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 100
            };
            MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            BlockingPipeWriter subscriberWriter = new();
            MqttZeroClientConnection subscriberConnection = new(
                subscriberWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState receiver = new("receiver", options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "large/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null));
            receiver.Connection = subscriberConnection;
            AddSession(broker, receiver);
            RebuildRoutes(broker);

            using CancellationTokenSource pumpLifetime = new();
            MqttZeroEgressActor pump = broker.StartEgress(
                receiver,
                subscriberConnection,
                pumpLifetime.Token);
            TrackingPipeWriter publisherWriter = new();
            MqttZeroClientConnection publisherConnection = new(
                publisherWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("large/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[64 * 1024]),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            lease.PacketIdentifier = 7;
            Task stop = Task.CompletedTask;

            try
            {
                await broker.RoutePublishLeaseAsync(
                    publisher,
                    publisherConnection,
                    lease,
                    TestContext.Current.CancellationToken);
                await subscriberWriter.FlushStarted.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);

                stop = broker.StopAsync(
                    TestContext.Current.CancellationToken).AsTask();
                await stop.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);
            }
            finally
            {
                subscriberWriter.ReleaseFlush();
                if (!stop.IsCompleted)
                {
                    try
                    {
                        await stop.WaitAsync(
                            TimeSpan.FromSeconds(2),
                            CancellationToken.None);
                    }
                    catch (Exception)
                    {
                    }
                }

                await pump.StopAsync();
                lease.Release();
                await publisherConnection.DisposeAsync();
                await subscriberConnection.DisposeAsync();
                await broker.DisposeAsync();
            }
        }

        [Fact(Timeout = 5_000)]
        public async Task QosZeroPublisherContinuesWhileSubscriberFlushIsBlocked()
        {
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 100
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            BlockingPipeWriter subscriberWriter = new();
            MqttZeroClientConnection subscriberConnection = new(
                subscriberWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState receiver = new(
                "receiver",
                options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "large/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            receiver.Connection = subscriberConnection;
            AddSession(broker, receiver);
            RebuildRoutes(broker);

            using CancellationTokenSource pumpLifetime = new();
            MqttZeroEgressActor pump = broker.StartEgress(
                receiver,
                subscriberConnection,
                pumpLifetime.Token);
            TrackingPipeWriter publisherWriter = new();
            MqttZeroClientConnection publisherConnection = new(
                publisherWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");
            MqttZeroMessageLease first = CreateQosZeroLease();
            MqttZeroMessageLease second = CreateQosZeroLease();

            try
            {
                await broker.RoutePublishLeaseAsync(
                    publisher,
                    publisherConnection,
                    first,
                    TestContext.Current.CancellationToken);
                await subscriberWriter.FlushStarted.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);

                await broker.RoutePublishLeaseAsync(
                    publisher,
                    publisherConnection,
                    second,
                    TestContext.Current.CancellationToken).AsTask().WaitAsync(
                        TimeSpan.FromSeconds(1),
                        TestContext.Current.CancellationToken);
            }
            finally
            {
                subscriberWriter.ReleaseFlush();
                await pump.StopAsync();
                first.Release();
                second.Release();
                await publisherConnection.DisposeAsync();
                await subscriberConnection.DisposeAsync();
            }
        }

        [Fact(Timeout = 5_000)]
        public async Task PendingQosZeroMessagesShareOneFlush()
        {
            const int messageCount = 3;
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 10,
                OutboundBatchDuration = TimeSpan.FromSeconds(1)
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            TrackingPipeWriter subscriberWriter = new()
            {
                CompletionWrittenBytes = messageCount * 1024
            };
            MqttZeroClientConnection subscriberConnection = new(
                subscriberWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState receiver = new(
                "receiver",
                options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "batch/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            receiver.Connection = subscriberConnection;
            AddSession(broker, receiver);
            RebuildRoutes(broker);
            TrackingPipeWriter publisherWriter = new();
            MqttZeroClientConnection publisherConnection = new(
                publisherWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");

            for (int index = 0; index < messageCount; index++)
            {
                MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    new ReadOnlySequence<byte>("batch/topic"u8.ToArray()),
                    ReadOnlySequence<byte>.Empty,
                    new ReadOnlySequence<byte>(new byte[1024]),
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

            using CancellationTokenSource pumpLifetime = new();
            MqttZeroEgressActor pump = broker.StartEgress(
                receiver,
                subscriberConnection,
                pumpLifetime.Token);
            try
            {
                await subscriberWriter.Completion.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);
                Assert.Equal(1, subscriberWriter.FlushCount);
            }
            finally
            {
                await pump.StopAsync();
                await publisherConnection.DisposeAsync();
                await subscriberConnection.DisposeAsync();
            }
        }

        [Fact(Timeout = 5_000)]
        public async Task PendingMessagesResumeAfterBatchLimitedAsyncFlush()
        {
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 10,
                OutboundBatchMessages = 1,
                OutboundBatchDuration = TimeSpan.FromSeconds(1)
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            BlockingPipeWriter subscriberWriter = new();
            MqttZeroClientConnection subscriberConnection = new(
                subscriberWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState receiver = new(
                "receiver",
                options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "batch/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            receiver.Connection = subscriberConnection;
            AddSession(broker, receiver);
            RebuildRoutes(broker);
            TrackingPipeWriter publisherWriter = new();
            MqttZeroClientConnection publisherConnection = new(
                publisherWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");

            for (int index = 0; index < 2; index++)
            {
                MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    new ReadOnlySequence<byte>("batch/topic"u8.ToArray()),
                    ReadOnlySequence<byte>.Empty,
                    new ReadOnlySequence<byte>(new byte[1024]),
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

            using CancellationTokenSource pumpLifetime = new();
            MqttZeroEgressActor pump = broker.StartEgress(
                receiver,
                subscriberConnection,
                pumpLifetime.Token);
            try
            {
                await subscriberWriter.FlushStarted.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);
                Assert.InRange(
                    subscriberWriter.WrittenBytesAtFirstFlush,
                    1024,
                    1500);
                subscriberWriter.ReleaseFlush();
                await subscriberWriter.SecondFlushStarted.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);
            }
            finally
            {
                await pump.StopAsync();
                await publisherConnection.DisposeAsync();
                await subscriberConnection.DisposeAsync();
            }
        }

        [Fact(Timeout = 5_000)]
        public async Task OutboundBatchMessagesHonorsConfiguredLimit()
        {
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 10,
                OutboundBatchMessages = 2
            };

            await AssertQosZeroBatchFlushCountAsync(
                options,
                messageCount: 5,
                payloadSize: 1024,
                expectedFlushCount: 3,
                TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 5_000)]
        public async Task OutboundBatchBytesUsesEncodedLength()
        {
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 10,
                OutboundBatchBytes = 1500,
                OutboundBatchMessages = 10
            };

            await AssertQosZeroBatchFlushCountAsync(
                options,
                messageCount: 5,
                payloadSize: 1024,
                expectedFlushCount: 5,
                TestContext.Current.CancellationToken);
        }

        [Fact(Timeout = 5_000)]
        public async Task LargeMessageBacklogHonorsEncodedByteLimit()
        {
            const int messageCount = 8;
            const int payloadSize = 64 * 1024;
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 20
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            TrackingPipeWriter subscriberWriter = new()
            {
                CompletionWrittenBytes = messageCount * payloadSize
            };
            MqttZeroClientConnection subscriberConnection = new(
                subscriberWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                2 * 1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState receiver = new(
                "receiver",
                options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "batch/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            receiver.Connection = subscriberConnection;
            AddSession(broker, receiver);
            RebuildRoutes(broker);
            TrackingPipeWriter publisherWriter = new();
            MqttZeroClientConnection publisherConnection = new(
                publisherWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                2 * 1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");

            for (int index = 0; index < messageCount; index++)
            {
                MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    new ReadOnlySequence<byte>("batch/topic"u8.ToArray()),
                    ReadOnlySequence<byte>.Empty,
                    new ReadOnlySequence<byte>(new byte[payloadSize]),
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

            using CancellationTokenSource pumpLifetime = new();
            MqttZeroEgressActor pump = broker.StartEgress(
                receiver,
                subscriberConnection,
                pumpLifetime.Token);
            try
            {
                await subscriberWriter.Completion.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);
                Assert.Equal(2, subscriberWriter.FlushCount);
            }
            finally
            {
                await pump.StopAsync();
                await publisherConnection.DisposeAsync();
                await subscriberConnection.DisposeAsync();
            }
        }

        [Fact(Timeout = 5_000)]
        public async Task ConcurrentControlPacketRegistersPriorityWaiter()
        {
            BlockingPipeWriter writer = new();
            MqttZeroClientConnection connection = new(
                writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("priority/topic"u8.ToArray()),
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
            try
            {
                Task publish = connection.SendPublishAsync(
                    message,
                    false,
                    TestContext.Current.CancellationToken).AsTask();
                await writer.FlushStarted.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);
                Task subAck = connection.SendSubAckAsync(
                    new MqttSubAckPacket
                    {
                        PacketIdentifier = 1,
                        ReasonCodes = new byte[] { 0 }
                    },
                    TestContext.Current.CancellationToken).AsTask();
                await Task.Delay(
                    TimeSpan.FromMilliseconds(50),
                    TestContext.Current.CancellationToken);
                writer.ReleaseFlush();
                await Task.WhenAll(publish, subAck).WaitAsync(
                    TimeSpan.FromSeconds(2),
                    TestContext.Current.CancellationToken);
                Assert.False(connection.HasPendingControlWrites);
            }
            finally
            {
                writer.ReleaseFlush();
                message.Release();
                lease.Release();
                await connection.DisposeAsync();
            }
        }

        [Fact(Timeout = 5_000)]
        public async Task AttachedConnectionRoutesControlPacketsThroughEgress()
        {
            await using MqttZeroEventLoop shard = new(0, 32);
            await using MqttZeroEgressScheduler scheduler = new(
                shard,
                diagnostics: null);
            TrackingPipeWriter writer = new();
            MqttZeroClientConnection connection = new(
                writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            int egressBatchCount = 0;
            await using MqttZeroEgressActor egress = new(
                scheduler,
                async (frames, cancellationToken) =>
                {
                    Interlocked.Increment(ref egressBatchCount);
                    await Task.CompletedTask.ConfigureAwait(false);
                },
                _ => ValueTask.FromResult(false),
                _ => { });

            try
            {
                connection.AttachEgress(egress);

                await connection.SendPubAckAsync(
                    1,
                    MqttReasonCode.Success,
                    TestContext.Current.CancellationToken);
                await connection.SendConnAckAsync(
                    new MqttConnAckPacket
                    {
                        ReasonCode = MqttReasonCode.Success
                    },
                    TestContext.Current.CancellationToken);
                await connection.SendDisconnectAsync(
                    new MqttDisconnectPacket
                    {
                        ReasonCode = MqttReasonCode.Success
                    },
                    TestContext.Current.CancellationToken);

                Assert.Equal(3, Volatile.Read(ref egressBatchCount));
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }

        [Fact]
        public async Task QosZeroOfflineDropReleasesOwnedLease()
        {
            const int payloadSize = 1024;
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 10,
                StoreQos0InQueue = false
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            MqttZeroBufferPool bufferPool = GetBufferPool(broker);
            MqttZeroSessionState receiver = new(
                "offline-qos-zero",
                options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "drop/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            AddSession(broker, receiver);
            RebuildRoutes(broker);
            MqttZeroClientConnection publisherConnection = new(
                new TrackingPipeWriter(),
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");
            MqttZeroMessageLease lease = CreateQosZeroLease(
                bufferPool,
                "drop/topic",
                payloadSize);
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
                await publisherConnection.DisposeAsync();
            }

            Assert.Equal(0, receiver.PendingMessages.Count);
            Assert.Equal(0, bufferPool.RentedBytes);
        }

        [Fact]
        public async Task QueueEvictionReleasesOwnedLease()
        {
            const int payloadSize = 1024;
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 1,
                StoreQos0InQueue = true
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            MqttZeroBufferPool bufferPool = GetBufferPool(broker);
            MqttZeroSessionState receiver = new(
                "evicting-qos-zero",
                options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "evict/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            AddSession(broker, receiver);
            RebuildRoutes(broker);
            MqttZeroClientConnection publisherConnection = new(
                new TrackingPipeWriter(),
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");
            MqttZeroMessageLease first = CreateQosZeroLease(
                bufferPool,
                "evict/topic",
                payloadSize);
            MqttZeroMessageLease second = CreateQosZeroLease(
                bufferPool,
                "evict/topic",
                payloadSize);
            bool firstReleased = false;
            bool secondReleased = false;
            try
            {
                await broker.RoutePublishLeaseAsync(
                    publisher,
                    publisherConnection,
                    first,
                    TestContext.Current.CancellationToken);
                first.Release();
                firstReleased = true;
                await broker.RoutePublishLeaseAsync(
                    publisher,
                    publisherConnection,
                    second,
                    TestContext.Current.CancellationToken);
                second.Release();
                secondReleased = true;

                Assert.Equal(1, receiver.PendingMessages.Count);
                lock (receiver.SyncRoot)
                {
                    MqttZeroQueuedMessage queued =
                        receiver.PendingMessages.Dequeue();
                    queued.Release();
                }

                Assert.Equal(0, bufferPool.RentedBytes);
            }
            finally
            {
                if (!firstReleased)
                {
                    first.Release();
                }

                if (!secondReleased)
                {
                    second.Release();
                }

                await publisherConnection.DisposeAsync();
            }
        }

        [Fact]
        public async Task ConnectionSwapBetweenDeliveriesUsesCurrentMaximumPacketSize()
        {
            const int payloadSize = 64 * 1024;
            MqttZeroOptions options = new()
            {
                MaxMessageQueueLength = 4,
                StoreQos0InQueue = true
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            MqttZeroBufferPool bufferPool = GetBufferPool(broker);
            MqttZeroSessionState receiver = new(
                "swapping-qos-zero",
                options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "swap/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            MqttZeroClientConnection firstConnection = new(
                new TrackingPipeWriter(),
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                2 * payloadSize,
                TimeProvider.System);
            MqttZeroClientConnection secondConnection = new(
                new TrackingPipeWriter(),
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                2 * payloadSize,
                TimeProvider.System);
            receiver.Connection = firstConnection;
            AddSession(broker, receiver);
            RebuildRoutes(broker);
            MqttZeroClientConnection publisherConnection = new(
                new TrackingPipeWriter(),
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                2 * payloadSize,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");
            MqttZeroMessageLease first = CreateQosZeroLease(
                bufferPool,
                "swap/topic",
                payloadSize);
            MqttZeroMessageLease second = CreateQosZeroLease(
                bufferPool,
                "swap/topic",
                payloadSize);
            try
            {
                await broker.RoutePublishLeaseAsync(
                    publisher,
                    publisherConnection,
                    first,
                    TestContext.Current.CancellationToken);
                lock (receiver.SyncRoot)
                {
                    receiver.Connection = secondConnection;
                }

                await broker.RoutePublishLeaseAsync(
                    publisher,
                    publisherConnection,
                    second,
                    TestContext.Current.CancellationToken);

                Assert.Equal(2, receiver.PendingMessages.Count);
                lock (receiver.SyncRoot)
                {
                    while (receiver.PendingMessages.Count > 0)
                    {
                        MqttZeroQueuedMessage queued =
                            receiver.PendingMessages.Dequeue();
                        queued.Release();
                    }
                }
            }
            finally
            {
                first.Release();
                second.Release();
                await publisherConnection.DisposeAsync();
                await firstConnection.DisposeAsync();
                await secondConnection.DisposeAsync();
            }
        }

        private static MqttZeroMessageLease CreateQosZeroLease()
        {
            return MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("large/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[64 * 1024]),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
        }

        private static MqttZeroMessageLease CreateQosZeroLease(
            MqttZeroBufferPool bufferPool,
            string topic,
            int payloadSize)
        {
            return MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>(System.Text.Encoding.UTF8.GetBytes(topic)),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[payloadSize]),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null,
                bufferPool);
        }

        private static MqttZeroBufferPool GetBufferPool(MqttZeroBroker broker)
        {
            FieldInfo bufferPoolField = typeof(MqttZeroBroker).GetField(
                "_bufferPool",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            return (MqttZeroBufferPool)bufferPoolField.GetValue(broker)!;
        }

        private static async Task AssertQosZeroBatchFlushCountAsync(
            MqttZeroOptions options,
            int messageCount,
            int payloadSize,
            int expectedFlushCount,
            CancellationToken cancellationToken)
        {
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
            await broker.StartAsync(cancellationToken);
            TrackingPipeWriter subscriberWriter = new()
            {
                CompletionWrittenBytes = messageCount * payloadSize
            };
            MqttZeroClientConnection subscriberConnection = new(
                subscriberWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState receiver = new(
                "receiver",
                options.MaxMessageQueueLength);
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "batch/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            receiver.Connection = subscriberConnection;
            AddSession(broker, receiver);
            RebuildRoutes(broker);
            TrackingPipeWriter publisherWriter = new();
            MqttZeroClientConnection publisherConnection = new(
                publisherWriter,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState publisher = new("publisher");

            await QueueQosZeroMessagesAsync(
                broker,
                publisher,
                publisherConnection,
                messageCount,
                payloadSize,
                cancellationToken);

            using CancellationTokenSource pumpLifetime = new();
            MqttZeroEgressActor pump = broker.StartEgress(
                receiver,
                subscriberConnection,
                pumpLifetime.Token);
            try
            {
                await subscriberWriter.Completion.WaitAsync(
                    TimeSpan.FromSeconds(2),
                    cancellationToken);
                Assert.Equal(
                    expectedFlushCount,
                    subscriberWriter.FlushCount);
            }
            finally
            {
                await pump.StopAsync();
                await publisherConnection.DisposeAsync();
                await subscriberConnection.DisposeAsync();
            }
        }

        private static async Task QueueQosZeroMessagesAsync(
            MqttZeroBroker broker,
            MqttZeroSessionState publisher,
            MqttZeroClientConnection publisherConnection,
            int messageCount,
            int payloadSize,
            CancellationToken cancellationToken)
        {
            for (int index = 0; index < messageCount; index++)
            {
                MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    new ReadOnlySequence<byte>("batch/topic"u8.ToArray()),
                    ReadOnlySequence<byte>.Empty,
                    new ReadOnlySequence<byte>(new byte[payloadSize]),
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
                        cancellationToken);
                }
                finally
                {
                    lease.Release();
                }
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

        private static void RebuildRoutes(MqttZeroBroker broker)
        {
            broker.ResetRouteEpochFromSessions();
        }

        private sealed class BlockingPipeWriter : PipeWriter
        {
            private readonly TaskCompletionSource _flushRelease = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _flushStarted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _secondFlushStarted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private byte[] _buffer = new byte[1024];
            private int _blockNextPublish = 1;
            private int _flushCount;
            private int _firstFlushWrittenBytes;
            private int _flushOffset;
            private int _written;

            public Task FlushStarted
            {
                get
                {
                    return _flushStarted.Task;
                }
            }

            public Task SecondFlushStarted
            {
                get
                {
                    return _secondFlushStarted.Task;
                }
            }

            public void ReleaseFlush()
            {
                _flushRelease.TrySetResult();
            }

            public int WrittenBytesAtFirstFlush
            {
                get
                {
                    return Volatile.Read(ref _firstFlushWrittenBytes);
                }
            }

            public override void Advance(int bytes)
            {
                _written += bytes;
            }

            public override void CancelPendingFlush()
            {
                ReleaseFlush();
            }

            public override void Complete(Exception? exception = null)
            {
            }

            public override ValueTask CompleteAsync(Exception? exception = null)
            {
                return ValueTask.CompletedTask;
            }

            public override async ValueTask<FlushResult> FlushAsync(
                CancellationToken cancellationToken = default)
            {
                if (Interlocked.Increment(ref _flushCount) == 2)
                {
                    _secondFlushStarted.TrySetResult();
                }
                else
                {
                    Volatile.Write(
                        ref _firstFlushWrittenBytes,
                        _written);
                }

                bool block = _written > _flushOffset &&
                    (_buffer[_flushOffset] >> 4) == 3 &&
                    Interlocked.Exchange(ref _blockNextPublish, 0) != 0;
                _flushOffset = _written;
                if (!block)
                {
                    return new FlushResult(false, false);
                }

                _flushStarted.TrySetResult();
                await _flushRelease.Task.WaitAsync(cancellationToken);
                return new FlushResult(false, false);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                EnsureCapacity(sizeHint);
                return _buffer.AsMemory(_written);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                EnsureCapacity(sizeHint);
                return _buffer.AsSpan(_written);
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                EnsureCapacity(source.Length);
                source.Span.CopyTo(_buffer.AsSpan(_written));
                _written += source.Length;
                return FlushAsync(cancellationToken);
            }

            private void EnsureCapacity(int sizeHint)
            {
                int required = _written + Math.Max(sizeHint, 1);
                if (required <= _buffer.Length)
                {
                    return;
                }

                Array.Resize(ref _buffer, Math.Max(required, _buffer.Length * 2));
            }
        }

        private sealed class TrackingPipeWriter : PipeWriter
        {
            private readonly TaskCompletionSource _completion = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private byte[] _buffer = new byte[1024];
            private int _written;

            public Task Completion
            {
                get
                {
                    return _completion.Task;
                }
            }

            public int CompletionWrittenBytes { get; set; } = int.MaxValue;

            public int FlushCount { get; private set; }

            public ReadOnlySequence<byte> WrittenSequence
            {
                get
                {
                    return new ReadOnlySequence<byte>(
                        _buffer.AsMemory(0, _written));
                }
            }

            public override void Advance(int bytes)
            {
                _written += bytes;
            }

            public override void CancelPendingFlush()
            {
            }

            public override void Complete(Exception? exception = null)
            {
            }

            public override ValueTask CompleteAsync(Exception? exception = null)
            {
                return ValueTask.CompletedTask;
            }

            public override ValueTask<FlushResult> FlushAsync(
                CancellationToken cancellationToken = default)
            {
                FlushCount++;
                if (_written >= CompletionWrittenBytes)
                {
                    _completion.TrySetResult();
                }

                return ValueTask.FromResult(new FlushResult(false, false));
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                EnsureCapacity(sizeHint);
                return _buffer.AsMemory(_written);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                EnsureCapacity(sizeHint);
                return _buffer.AsSpan(_written);
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                EnsureCapacity(source.Length);
                source.Span.CopyTo(_buffer.AsSpan(_written));
                _written += source.Length;
                return ValueTask.FromResult(new FlushResult(false, false));
            }

            private void EnsureCapacity(int sizeHint)
            {
                int required = _written + Math.Max(sizeHint, 1);
                if (required <= _buffer.Length)
                {
                    return;
                }

                Array.Resize(ref _buffer, Math.Max(required, _buffer.Length * 2));
            }
        }
    }
}
