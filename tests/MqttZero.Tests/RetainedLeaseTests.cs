using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Reflection;

namespace MqttZero.Tests
{
    public sealed class RetainedLeaseTests
    {
        [Fact]
        public async Task ClearingRetainedMessageReleasesPooledLease()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            MqttZeroBufferPool bufferPool = Assert.IsType<MqttZeroBufferPool>(
                typeof(MqttZeroBroker).GetField(
                    "_bufferPool",
                    BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(broker));

            await broker.PublishAsync(
                "retained/topic",
                new ReadOnlySequence<byte>("retained-payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);
            Assert.True(bufferPool.RentedBytes > 0);

            await broker.PublishAsync(
                "retained/topic",
                new ReadOnlySequence<byte>("replacement"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);

            await broker.PublishAsync(
                "retained/topic",
                ReadOnlySequence<byte>.Empty,
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);

            Assert.Equal(0, bufferPool.RentedBytes);
            await broker.DisposeAsync();
        }
    }
}
