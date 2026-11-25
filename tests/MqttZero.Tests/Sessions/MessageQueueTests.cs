using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Tests.Sessions
{
    public sealed class MessageQueueTests
    {
        [Fact]
        public void LargeCapacityDoesNotPreallocateCandidateStorage()
        {
            long before = GC.GetAllocatedBytesForCurrentThread();

            _ = new MqttZeroMessageQueue(1_000_000);

            long allocated =
                GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.InRange(allocated, 0, 1024);
        }

        [Fact]
        public void DequeueRemovesQosZeroFromEvictionCandidates()
        {
            MqttZeroMessageQueue queue = new(4);
            MqttZeroQueuedMessage firstQos0 = CreateQueuedMessage(
                MqttQualityOfServiceLevel.AtMostOnce,
                out MqttZeroMessageLease firstLease);
            MqttZeroQueuedMessage secondQos0 = CreateQueuedMessage(
                MqttQualityOfServiceLevel.AtMostOnce,
                out MqttZeroMessageLease secondLease);
            queue.Add(firstQos0);
            queue.Add(secondQos0);
            _ = queue.Dequeue();

            MqttZeroQueuedMessage? evicted =
                queue.EvictOldestQos0OrOldest();

            Assert.Same(secondQos0, evicted);
            firstQos0.Release();
            evicted!.Release();
            firstLease.Release();
            secondLease.Release();
        }

        [Fact]
        public void RemoveAtDecrementsExpiringCountWhenRemovedMessageExpires()
        {
            MqttZeroMessageQueue queue = new(4);
            MqttZeroQueuedMessage expiring = CreateQueuedMessage(
                DateTimeOffset.UtcNow.AddMinutes(1),
                out MqttZeroMessageLease lease);
            queue.Add(expiring);

            queue.RemoveAt(0);

            Assert.False(queue.HasExpiringMessages);
            expiring.Release();
            lease.Release();
        }

        [Fact]
        public void RemoveAtKeepsExpiringCountWhenShiftedMessageExpires()
        {
            MqttZeroMessageQueue queue = new(4);
            MqttZeroQueuedMessage persistent = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease persistentLease);
            MqttZeroQueuedMessage expiring = CreateQueuedMessage(
                DateTimeOffset.UtcNow.AddMinutes(1),
                out MqttZeroMessageLease expiringLease);
            queue.Add(persistent);
            queue.Add(expiring);

            queue.RemoveAt(0);

            Assert.True(queue.HasExpiringMessages);
            Assert.Same(expiring, queue.Peek());
            persistent.Release();
            expiring.Release();
            persistentLease.Release();
            expiringLease.Release();
        }

        [Fact]
        public void EmptyQueueRejectsPeekAndDequeue()
        {
            MqttZeroMessageQueue queue = new(2);

            Assert.Throws<InvalidOperationException>(() => queue.Peek());
            Assert.Throws<InvalidOperationException>(() => queue.Dequeue());
        }

        [Fact]
        public void QueueRejectsFullAddAndPrepend()
        {
            MqttZeroMessageQueue queue = new(1);
            MqttZeroQueuedMessage first = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease firstLease);
            MqttZeroQueuedMessage second = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease secondLease);
            queue.Add(first);

            Assert.Throws<InvalidOperationException>(() => queue.Add(second));
            Assert.Throws<InvalidOperationException>(() => queue.Prepend(second));
            first.Release();
            second.Release();
            firstLease.Release();
            secondLease.Release();
        }

        [Fact]
        public void QueueWrapsAfterDequeueAndPrepend()
        {
            MqttZeroMessageQueue queue = new(3);
            MqttZeroQueuedMessage first = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease firstLease);
            MqttZeroQueuedMessage second = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease secondLease);
            MqttZeroQueuedMessage third = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease thirdLease);
            queue.Add(first);
            queue.Add(second);
            Assert.Same(first, queue.Dequeue());
            queue.Prepend(first);
            queue.Add(third);

            List<MqttZeroQueuedMessage> actual = [];
            foreach (MqttZeroQueuedMessage message in queue)
            {
                actual.Add(message);
            }

            Assert.Equal([first, second, third], actual);
            while (queue.Count > 0)
            {
                queue.Dequeue().Release();
            }

            firstLease.Release();
            secondLease.Release();
            thirdLease.Release();
        }

        [Fact]
        public void ClearResetsQueueCapacityAndExpiringState()
        {
            MqttZeroMessageQueue queue = new(1);
            MqttZeroQueuedMessage expiring = CreateQueuedMessage(
                DateTimeOffset.UtcNow.AddMinutes(1),
                out MqttZeroMessageLease lease);
            queue.Add(expiring);

            queue.Clear();

            Assert.Equal(0, queue.Count);
            Assert.False(queue.HasExpiringMessages);
            expiring.Release();
            lease.Release();
        }

        [Fact]
        public void RemoveExpiredReturnsExpiredMessagesAndKeepsLiveMessages()
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            MqttZeroMessageQueue queue = new(3);
            MqttZeroQueuedMessage expired = CreateQueuedMessage(
                now.AddSeconds(-1),
                out MqttZeroMessageLease expiredLease);
            MqttZeroQueuedMessage live = CreateQueuedMessage(
                now.AddSeconds(30),
                out MqttZeroMessageLease liveLease);
            queue.Add(expired);
            queue.Add(live);
            List<MqttZeroQueuedMessage> removed = [];

            queue.RemoveExpired(now, removed);

            Assert.Equal([expired], removed);
            Assert.Equal(1, queue.Count);
            Assert.Same(live, queue.Peek());
            expired.Release();
            live.Release();
            expiredLease.Release();
            liveLease.Release();
        }

        [Fact]
        public void DequeueExpiringMessageUpdatesExpiringCount()
        {
            MqttZeroMessageQueue queue = new(2);
            MqttZeroQueuedMessage expiring = CreateQueuedMessage(
                DateTimeOffset.UtcNow.AddMinutes(1),
                out MqttZeroMessageLease lease);
            queue.Add(expiring);

            _ = queue.Dequeue();

            Assert.False(queue.HasExpiringMessages);
            expiring.Release();
            lease.Release();
        }

        [Fact]
        public void PrependExpiringMessageUpdatesExpiringCount()
        {
            MqttZeroMessageQueue queue = new(2);
            MqttZeroQueuedMessage expiring = CreateQueuedMessage(
                DateTimeOffset.UtcNow.AddMinutes(1),
                out MqttZeroMessageLease lease);

            queue.Prepend(expiring);

            Assert.True(queue.HasExpiringMessages);
            Assert.Same(expiring, queue.Dequeue());
            expiring.Release();
            lease.Release();
        }

        [Fact]
        public void FindIndexReturnsMatchingPositionOrMinusOne()
        {
            MqttZeroMessageQueue queue = new(2);
            MqttZeroQueuedMessage first = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease firstLease);
            MqttZeroQueuedMessage second = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease secondLease);
            queue.Add(first);
            queue.Add(second);

            Assert.Equal(1, queue.FindIndex(message => ReferenceEquals(message, second)));
            Assert.Equal(-1, queue.FindIndex(static _ => false));
            first.Release();
            second.Release();
            firstLease.Release();
            secondLease.Release();
        }

        [Fact]
        public void IndexerRejectsNegativeAndOutOfRangeIndices()
        {
            MqttZeroMessageQueue queue = new(1);
            MqttZeroQueuedMessage message = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease lease);
            queue.Add(message);

            Assert.Throws<ArgumentOutOfRangeException>(() => _ = queue[-1]);
            Assert.Throws<ArgumentOutOfRangeException>(() => _ = queue[1]);
            message.Release();
            lease.Release();
        }

        [Fact]
        public void EvictsOldestQos0BeforeOtherMessages()
        {
            MqttZeroMessageQueue queue = new(3);
            MqttZeroQueuedMessage qos1 = CreateQueuedMessage(
                MqttQualityOfServiceLevel.AtLeastOnce,
                out MqttZeroMessageLease qos1Lease);
            MqttZeroQueuedMessage qos0 = CreateQueuedMessage(
                MqttQualityOfServiceLevel.AtMostOnce,
                out MqttZeroMessageLease qos0Lease);
            queue.Add(qos1);
            queue.Add(qos0);

            MqttZeroQueuedMessage? evicted =
                queue.EvictOldestQos0OrOldest();

            Assert.Same(qos0, evicted);
            Assert.Same(qos1, queue.Peek());
            evicted!.Release();
            qos1.Release();
            qos0Lease.Release();
            qos1Lease.Release();
        }

        [Fact]
        public void RejectsAddingTheSameSingleItemTwice()
        {
            MqttZeroMessageQueue queue = new(2);
            MqttZeroQueuedMessage message = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease lease);
            queue.Add(message);

            Assert.Throws<InvalidOperationException>(
                () => queue.Add(message));
            Assert.Equal(1, queue.Count);
            queue.Dequeue().Release();
            lease.Release();
        }

        private static MqttZeroQueuedMessage CreateQueuedMessage(
            DateTimeOffset? expiresAt,
            out MqttZeroMessageLease lease)
        {
            return CreateQueuedMessage(
                MqttQualityOfServiceLevel.AtLeastOnce,
                out lease,
                expiresAt);
        }

        private static MqttZeroQueuedMessage CreateQueuedMessage(
            MqttQualityOfServiceLevel qualityOfServiceLevel,
            out MqttZeroMessageLease lease,
            DateTimeOffset? expiresAt = null)
        {
            lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("queue/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                qualityOfServiceLevel,
                false,
                null,
                null);
            return MqttZeroQueuedMessage.Rent(
                lease,
                qualityOfServiceLevel,
                false,
                null,
                expiresAt);
        }
    }
}
