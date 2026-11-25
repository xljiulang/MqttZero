using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Reflection;

namespace MqttZero.Tests.Sessions
{
    public sealed class SessionStateTests
    {
        [Fact]
        public void StoresAssignedShardId()
        {
            MqttZeroSessionState session = new(
                "sharded",
                maxQueueLength: 32,
                shardId: 7);

            Assert.Equal(7, session.ShardId);
        }

        [Fact]
        public void PacketIdentifierSkipsZeroAndInflightIdentifiers()
        {
            MqttZeroSessionState session = new("packet-id");
            FieldInfo nextField = typeof(MqttZeroSessionState).GetField(
                "_nextPacketIdentifier",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            nextField.SetValue(session, 0);
            session.InflightMessages.Add(1, null!);
            session.InflightMessages.Add(2, null!);

            Assert.Equal((ushort)3, session.GetNextPacketIdentifier());
        }

        [Fact]
        public void SnapshotCapturesExpiryWillAndCounts()
        {
            DateTimeOffset now = new(2026, 11, 24, 2, 0, 0, TimeSpan.Zero);
            MqttZeroSessionState session = new("snapshot")
            {
                CleanStart = false,
                ExpiryInterval = 60,
                DisconnectedAt = now.AddSeconds(-10),
                WillMessage = new MqttZeroWillMessage(
                    new MqttConnectPacket
                    {
                        HasWill = true,
                        WillTopic = new ReadOnlySequence<byte>("will/topic"u8.ToArray()),
                        WillPayload = new ReadOnlySequence<byte>("value"u8.ToArray())
                    })
            };
            session.Subscriptions.Add(new MqttZeroSubscription(
                "devices/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null));

            MqttZeroSessionSnapshot snapshot = session.CreateSnapshot(now);

            Assert.Equal("snapshot", snapshot.ClientId);
            Assert.False(snapshot.IsConnected);
            Assert.Equal(1, snapshot.SubscriptionCount);
            Assert.Equal(now.AddSeconds(50), snapshot.ExpiresAt);
            Assert.True(snapshot.HasWill);
            Assert.Equal("will/topic", snapshot.WillTopic);
            Assert.Equal(now, snapshot.SampledAt);
        }

        [Fact]
        public void SnapshotOmitsExpiryWhenSessionNeverExpires()
        {
            MqttZeroSessionState session = new("forever")
            {
                ExpiryInterval = 0,
                DisconnectedAt = DateTimeOffset.UtcNow
            };

            MqttZeroSessionSnapshot snapshot = session.CreateSnapshot(
                DateTimeOffset.UtcNow);

            Assert.Null(snapshot.ExpiresAt);
            Assert.False(snapshot.HasWill);
            Assert.Null(snapshot.WillTopic);
        }
    }
}
