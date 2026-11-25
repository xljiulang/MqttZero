using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Tests.Support;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;
using System.Runtime.InteropServices;


namespace MqttZero.Tests.Memory
{
    public sealed class MemoryLeaseAllocationTests
    {
        [Fact]
        public void OwnedLeaseStoresAllRegionsInOneSlab()
        {
            using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                new ReadOnlySequence<byte>([1, 2]),
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);

            Assert.True(MemoryMarshal.TryGetArray(
                lease.Topic,
                out ArraySegment<byte> topic));
            Assert.True(MemoryMarshal.TryGetArray(
                lease.Properties,
                out ArraySegment<byte> properties));
            Assert.True(MemoryMarshal.TryGetArray(
                lease.Payload,
                out ArraySegment<byte> payload));
            Assert.Same(topic.Array, properties.Array);
            Assert.Same(topic.Array, payload.Array);
            Assert.True(topic.Offset < properties.Offset);
            Assert.True(properties.Offset < payload.Offset);
        }

        [Fact]
        public void BorrowedLeasePromotesIntoOneSlab()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            SequenceSegment first = new("pay"u8.ToArray());
            SequenceSegment second = new("load"u8.ToArray());
            first.SetNext(second);
            MqttZeroMessageLease lease = MqttZeroMessageLease.BorrowFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                new ReadOnlySequence<byte>([1, 2]),
                new ReadOnlySequence<byte>(first, 0, second, second.Memory.Length),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null,
                pool);

            try
            {
                lease.EnsureOwned();
                Assert.True(MemoryMarshal.TryGetArray(
                    lease.Topic,
                    out ArraySegment<byte> topic));
                Assert.True(MemoryMarshal.TryGetArray(
                    lease.Properties,
                    out ArraySegment<byte> properties));
                Assert.True(MemoryMarshal.TryGetArray(
                    lease.Payload,
                    out ArraySegment<byte> payload));
                Assert.Same(topic.Array, properties.Array);
                Assert.Same(topic.Array, payload.Array);
                Assert.Equal("payload", System.Text.Encoding.UTF8.GetString(
                    lease.Payload.Span));
            }
            finally
            {
                lease.Release();
            }
        }

        [Fact]
        public void PooledMessageLeaseDoesNotAllocateAfterWarmup()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            ReadOnlySequence<byte> topic = new("hot/topic"u8.ToArray());
            ReadOnlySequence<byte> payload = new(new byte[8192]);
            ReadOnlySequence<byte> properties = ReadOnlySequence<byte>.Empty;

            using (MqttZeroMessageLease warmup = MqttZeroMessageLease.CopyFrom(
                topic,
                properties,
                payload,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null,
                pool))
            {
                Assert.Equal(8192, warmup.Payload.Length);
            }

            int total = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1_000; index++)
            {
                using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    topic,
                    properties,
                    payload,
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    false,
                    null,
                    null,
                    pool);
                total += lease.Payload.Length;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(8_192_000, total);
            Assert.Equal(0, allocated);
        }
        [Fact]
        public void MessageLeaseReturnsBufferOnlyAfterLastReference()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
            lease.Retain();

            lease.Release();
            Assert.Equal("payload", System.Text.Encoding.UTF8.GetString(lease.Payload.Span));
            lease.Release();
        }
        [Fact]
        public void MessageLeaseReturnsBuffersToInjectedPoolAfterLastReference()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null,
                pool);

            Assert.True(pool.RentedBytes > 0);
            lease.Release();

            Assert.Equal(0, pool.RentedBytes);
        }
        [Fact]
        public void MessageLeaseHandlesConcurrentReferencesAndFinalRelease()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null,
                pool);

            Parallel.For(0, 1000, _ => lease.Retain());
            Parallel.For(0, 1000, _ => lease.Release());
            Assert.True(pool.RentedBytes > 0);

            lease.Release();
            Assert.Equal(0, pool.RentedBytes);
        }
        [Fact]
        public void MessageLeaseRejectsDuplicateRelease()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);

            lease.Release();

            Assert.Throws<ObjectDisposedException>(() => lease.Release());
        }
        [Fact]
        public void BorrowedMessageLeaseDefersCopiesUntilOwnedCopyIsRequested()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroMessageLease borrowed = MqttZeroMessageLease.BorrowFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null,
                pool);

            Assert.True(borrowed.IsBorrowed);
            Assert.Equal(0, pool.RentedBytes);
            MqttZeroMessageLease owned = borrowed.CreateOwnedCopy();
            Assert.False(owned.IsBorrowed);
            Assert.True(pool.RentedBytes > 0);

            borrowed.Release();
            owned.Release();
            Assert.Equal(0, pool.RentedBytes);
        }

        [Fact]
        public void BorrowedMessageLeasePromotesOnceAndKeepsSharedReferences()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroMessageLease lease = MqttZeroMessageLease.BorrowFrom(
                new System.Buffers.ReadOnlySequence<byte>("topic"u8.ToArray()),
                System.Buffers.ReadOnlySequence<byte>.Empty,
                new System.Buffers.ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null,
                pool);
            lease.EnsureOwned();
            long rented = pool.RentedBytes;
            lease.Retain();
            lease.EnsureOwned();

            Assert.False(lease.IsBorrowed);
            Assert.Equal(rented, pool.RentedBytes);
            lease.Release();
            lease.Release();
            Assert.Equal(0, pool.RentedBytes);
        }

        [Fact]
        public void MultipleQueuedMessagesShareOneOwnedLease()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroMessageLease lease = MqttZeroMessageLease.BorrowFrom(
                new System.Buffers.ReadOnlySequence<byte>("topic"u8.ToArray()),
                System.Buffers.ReadOnlySequence<byte>.Empty,
                new System.Buffers.ReadOnlySequence<byte>(new byte[8192]),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null,
                pool);
            MqttZeroQueuedMessage first = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            long rented = pool.RentedBytes;
            MqttZeroQueuedMessage second = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);

            Assert.Same(first.Lease, second.Lease);
            Assert.Equal(rented, pool.RentedBytes);
            first.Release();
            second.Release();
            lease.Release();
            Assert.Equal(0, pool.RentedBytes);
        }
        [Fact]
        public void QueuedMessageRejectsDuplicateRelease()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
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

            message.Release();
            lease.Release();

            Assert.Throws<ObjectDisposedException>(() => message.Release());
        }
        [Fact]
        public void PooledQosOneQueueAndInflightDoNotAllocateAfterWarmup()
        {
            MqttZeroSessionState session = new("qos-one");
            ReadOnlySequence<byte> topic = new("hot/topic"u8.ToArray());
            ReadOnlySequence<byte> payload = new(new byte[1024]);

            for (int index = 0; index < 10; index++)
            {
                ExerciseQosOneQueueAndInflight(session, topic, payload);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                ExerciseQosOneQueueAndInflight(session, topic, payload);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(0, session.PendingMessages.Count);
            Assert.Empty(session.InflightMessages);
            Assert.Equal(0, allocated);
        }
        private static void ExerciseQosOneQueueAndInflight(
            MqttZeroSessionState session,
            ReadOnlySequence<byte> topic,
            ReadOnlySequence<byte> payload)
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                topic,
                ReadOnlySequence<byte>.Empty,
                payload,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            MqttZeroQueuedMessage queued = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            session.PendingMessages.Add(queued);
            session.PendingMessages.RemoveAt(0);
            queued.PacketIdentifier = 1;
            session.InflightMessages.Add(1, queued);
            session.InflightMessages.Remove(1, out MqttZeroQueuedMessage? inflight);
            Assert.NotNull(inflight);
            inflight.Release();
            lease.Release();
        }

    }
}
