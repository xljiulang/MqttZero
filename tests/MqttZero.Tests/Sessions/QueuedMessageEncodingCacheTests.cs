using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Tests.Sessions
{
    public sealed class QueuedMessageEncodingCacheTests
    {
        [Fact]
        public void EncodingCacheIsResetWhenMessageReturnsToPool()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("cache/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[32]),
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
            message.SetEncodedLength(123, 1024);

            Assert.Equal(123, message.EncodedLength);
            Assert.True(message.IsEncodedLengthValidFor(1024));
            message.Release();

            MqttZeroQueuedMessage reused = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
            Assert.Equal(0, reused.EncodedLength);
            Assert.False(reused.IsEncodedLengthValidFor(1024));
            reused.Release();
            lease.Release();
        }
    }
}
