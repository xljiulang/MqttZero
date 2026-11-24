using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Routing;
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
            MqttZeroSessionOutboundPump pump = broker.StartSessionOutboundPump(
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
            MqttZeroSessionOutboundPump pump = broker.StartSessionOutboundPump(
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
            MqttZeroSessionOutboundPump pump = broker.StartSessionOutboundPump(
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
                MaxMessageQueueLength = 10
            };
            await using MqttZeroBroker broker = MqttZeroBroker.Create(
                options,
                TimeProvider.System);
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
            MqttZeroSessionOutboundPump pump = broker.StartSessionOutboundPump(
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
        public async Task LargeMessageBacklogUsesBoundedBatches()
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
            MqttZeroSessionOutboundPump pump = broker.StartSessionOutboundPump(
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
                Assert.True(connection.HasPendingControlWrites);
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
            MethodInfo rebuildMethod = typeof(MqttZeroBroker).GetMethod(
                "RebuildSubscriptionRouteSnapshot",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            rebuildMethod.Invoke(broker, null);
        }

        private sealed class BlockingPipeWriter : PipeWriter
        {
            private readonly TaskCompletionSource _flushRelease = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _flushStarted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private byte[] _buffer = new byte[1024];
            private int _blockNextPublish = 1;
            private int _flushOffset;
            private int _written;

            public Task FlushStarted
            {
                get
                {
                    return _flushStarted.Task;
                }
            }

            public void ReleaseFlush()
            {
                _flushRelease.TrySetResult();
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

            public override async ValueTask<FlushResult> FlushAsync(
                CancellationToken cancellationToken = default)
            {
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
