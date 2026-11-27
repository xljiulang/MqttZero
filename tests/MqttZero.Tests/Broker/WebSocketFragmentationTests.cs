using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Tests.Support;
using System.Buffers;
using System.Net;

namespace MqttZero.Tests.Broker
{
    public sealed class WebSocketFragmentationTests
    {
        [Fact]
        public async Task DisabledFragmentationWritesPublishAsOneTransportWrite()
        {
            AllocationPipeWriter writer = new(16 * 1024);
            MqttZeroClientConnection connection = new(
                writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                true,
                0,
                32,
                1024 * 1024,
                TimeProvider.System,
                allowPacketFragmentation: false);
            MqttZeroMessageLease lease = MqttZeroMessageLease.BorrowFrom(
                new ReadOnlySequence<byte>("frame/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[8192]),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
            try
            {
                await connection.TrySendPublishAsync(
                    lease,
                    MqttQualityOfServiceLevel.AtMostOnce,
                    false,
                    null,
                    0,
                    false,
                    TestContext.Current.CancellationToken);

                Assert.Equal(1, writer.WriteAsyncCount);
            }
            finally
            {
                lease.Release();
                await connection.DisposeAsync();
            }
        }
    }
}
