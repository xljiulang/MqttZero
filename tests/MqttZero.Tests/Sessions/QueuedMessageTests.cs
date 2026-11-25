using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Tests.Sessions
{
    public sealed class QueuedMessageTests
    {
        [Fact]
        public void CreatePacketIncludesAllSubscriptionIdentifiers()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            MqttZeroQueuedMessage message = MqttZeroQueuedMessage.RentWithIdentifiers(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                new uint[] { 7, 9 },
                null);

            try
            {
                MqttPublishPacket packet = message.CreatePacket(false);

                Assert.Equal(new uint[] { 7, 9 }, packet.SubscriptionIdentifiers);
            }
            finally
            {
                message.Release();
            }
        }

        [Fact]
        public void CopiedSubscriptionIdentifiersSurviveSourceMutation()
        {
            using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            uint[] source = ArrayPool<uint>.Shared.Rent(2);
            source[0] = 7;
            source[1] = 9;
            MqttZeroQueuedMessage message =
                MqttZeroQueuedMessage.RentWithCopiedIdentifiers(
                    lease,
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    false,
                    source.AsMemory(0, 2),
                    null);

            try
            {
                source[0] = 99;
                source[1] = 100;

                Assert.Equal(
                    new uint[] { 7, 9 },
                    message.SubscriptionIdentifiers.ToArray());
            }
            finally
            {
                message.Release();
                ArrayPool<uint>.Shared.Return(source);
            }
        }

        [Fact]
        public void PacketAndViewExposeMessageValues()
        {
            using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("queued/topic"u8.ToArray()),
                new ReadOnlySequence<byte>([0x01, 0x01]),
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.ExactlyOnce,
                true,
                null,
                7);
            MqttZeroQueuedMessage message = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.ExactlyOnce,
                true,
                5,
                DateTimeOffset.UtcNow.AddMinutes(1));
            try
            {
                message.PacketIdentifier = 9;

                MqttPublishPacket packet = message.CreatePacket(true);
                MqttZeroPublishView view = message.CreateView(true);

                Assert.Equal((ushort)9, packet.PacketIdentifier);
                Assert.True(packet.Duplicate);
                Assert.True(packet.Retain);
                Assert.Equal((ushort)9, view.PacketIdentifier);
                Assert.True(view.Retain);
            }
            finally
            {
                message.Release();
            }
        }

        [Fact]
        public void StateTransitionsAndRestoreAreApplied()
        {
            using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.ExactlyOnce,
                false,
                null,
                null);
            MqttZeroQueuedMessage message = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.ExactlyOnce,
                false,
                null,
                null);
            try
            {
                message.SetAwaitPubComp();
                Assert.Equal(MqttZeroOutboundState.AwaitPubComp, message.State);
                message.SetAwaitPubRec();
                Assert.Equal(MqttZeroOutboundState.AwaitPubRec, message.State);
                message.Restore(42, 7, MqttZeroOutboundState.AwaitPubComp);

                Assert.Equal(42, message.MessageId);
                Assert.Equal((ushort)7, message.PacketIdentifier);
                Assert.Equal(MqttZeroOutboundState.AwaitPubComp, message.State);
            }
            finally
            {
                message.Release();
            }
        }

        [Fact]
        public void ExpirationUsesInclusiveTimestamp()
        {
            DateTimeOffset now = DateTimeOffset.UtcNow;
            using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            MqttZeroQueuedMessage message = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                now);
            try
            {
                Assert.True(message.IsExpired(now));
                Assert.False(message.IsExpired(now.AddTicks(-1)));
            }
            finally
            {
                message.Release();
            }
        }
    }
}
