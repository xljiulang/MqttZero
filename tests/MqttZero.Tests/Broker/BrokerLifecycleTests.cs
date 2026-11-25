using MqttQualityOfServiceLevel = MqttZero.Protocol.MqttQualityOfServiceLevel;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Retained;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Tests.Support;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Reflection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using MqttZero.AspNetCore;

namespace MqttZero.Tests.Broker
{
    public sealed class BrokerLifecycleTests
    {
        [Fact(Timeout = 10_000)]
        public async Task BackgroundFanoutFailureStopsNewConnections()
        {
            ThrowingEnqueueInterceptor interceptor = new();
            await using MqttZeroBroker broker =
                MqttZeroBrokerTestFactory.CreateWithEnqueueInterceptor(
                    interceptor);
            await broker.StartAsync(TestContext.Current.CancellationToken);

            await PublishQosOneToSubscriberAsync(broker);
            await interceptor.WaitForFailureAsync();
            await WaitUntilAsync(
                () => broker.State != MqttZeroBrokerState.Running,
                TestContext.Current.CancellationToken);

            await broker.StopAsync(TestContext.Current.CancellationToken)
                .AsTask()
                .WaitAsync(
                    TimeSpan.FromSeconds(5),
                    TestContext.Current.CancellationToken);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
        }

        [Fact]
        public async Task LifecycleTransitionsAndSnapshotsAreAvailable()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);

            await broker.StartAsync(TestContext.Current.CancellationToken);

            Assert.Equal(MqttZeroBrokerState.Running, broker.State);
            Assert.Empty(broker.GetClients());

            await broker.PublishAsync(
                "devices/a",
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);
            MqttZeroRetainedMessageSnapshot retained = Assert.Single(broker.GetRetainedMessages());
            Assert.Equal("devices/a", retained.Topic);
            Assert.Equal(7, retained.PayloadLength);

            await broker.PublishAsync(
                "devices/a",
                ReadOnlySequence<byte>.Empty,
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);
            Assert.Empty(broker.GetRetainedMessages());

            await broker.StopAsync(TestContext.Current.CancellationToken);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
        }

        [Fact]
        public async Task HealthCheckReportsBrokerState()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            MqttZeroHealthCheck healthCheck = new(broker);

            HealthCheckResult stopped = await healthCheck.CheckHealthAsync(
                new HealthCheckContext(),
                TestContext.Current.CancellationToken);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            HealthCheckResult running = await healthCheck.CheckHealthAsync(
                new HealthCheckContext(),
                TestContext.Current.CancellationToken);

            Assert.Equal(HealthStatus.Unhealthy, stopped.Status);
            Assert.Equal(HealthStatus.Healthy, running.Status);
        }

        [Fact]
        public async Task BrokerCanRestartAfterStopButNotAfterDispose()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.StopAsync(TestContext.Current.CancellationToken);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(MqttZeroBrokerState.Running, broker.State);
            await broker.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(
                async () => await broker.StartAsync(
                    TestContext.Current.CancellationToken));
        }

        [Fact]
        public async Task BrokerStartsAndStopsShardSetWithLifecycle()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    EventLoopShardCount = 2
                },
                TimeProvider.System);

            await broker.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(2, broker.ShardCount);
            Assert.NotNull(broker.Shards);

            await broker.StopAsync(TestContext.Current.CancellationToken);
            Assert.Equal(0, broker.ShardCount);
            Assert.Null(broker.Shards);

            await broker.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(2, broker.ShardCount);
            await broker.DisposeAsync();
        }

        [Fact(Timeout = 10_000)]
        public async Task StopDrainsConnectionBeforeConnectCompletes()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            Pipe input = new();
            Pipe output = new();
            Task accept = broker.AcceptConnectionAsync(
                input.Reader,
                output.Writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                null,
                true,
                CancellationToken.None).AsTask();

            Assert.Equal(1, broker.ActiveConnectionCount);
            Assert.Equal(0, broker.ConnectedClientCount);
            await broker.StopAsync(TestContext.Current.CancellationToken);
            await accept;

            Assert.Equal(0, broker.ActiveConnectionCount);
            Assert.Equal(0, broker.ConnectedClientCount);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
            await broker.DisposeAsync();
        }

        [Fact(Timeout = 10_000)]
        public async Task ConcurrentStopAndDisposeDrainConnectedClient()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            Pipe input = new();
            Pipe output = new();
            Task accept = broker.AcceptConnectionAsync(
                input.Reader,
                output.Writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                null,
                true,
                CancellationToken.None).AsTask();
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("lifecycle-client"u8.ToArray()),
                    CleanStart = true
                },
                writer);
            await input.Writer.WriteAsync(
                writer.WrittenMemory,
                TestContext.Current.CancellationToken);
            _ = await ReadConnAckAsync(
                output.Reader,
                TestContext.Current.CancellationToken);
            await WaitForConnectedClientCountAsync(
                broker,
                1,
                TestContext.Current.CancellationToken);

            Task stop = broker.StopAsync(
                TestContext.Current.CancellationToken).AsTask();
            Task dispose = broker.DisposeAsync().AsTask();
            await Task.WhenAll(stop, dispose, accept);

            Assert.Equal(0, broker.ActiveConnectionCount);
            Assert.Equal(0, broker.ConnectedClientCount);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
        }

        [Fact(Timeout = 10_000)]
        public async Task ActiveConnectionLimitRejectsBeforeConnect()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    MaxActiveConnections = 1
                },
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            Pipe firstInput = new();
            Pipe firstOutput = new();
            Task firstAccept = broker.AcceptConnectionAsync(
                firstInput.Reader,
                firstOutput.Writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                null,
                true,
                CancellationToken.None).AsTask();
            Pipe secondInput = new();
            Pipe secondOutput = new();

            await broker.AcceptConnectionAsync(
                secondInput.Reader,
                secondOutput.Writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                null,
                true,
                CancellationToken.None);

            Assert.Equal(1, broker.ActiveConnectionCount);
            Assert.Equal(0, broker.ConnectedClientCount);
            await broker.StopAsync(TestContext.Current.CancellationToken);
            await firstAccept;
            await broker.DisposeAsync();
        }

        private static async Task<MqttConnAckPacket> ReadConnAckAsync(
            PipeReader reader,
            CancellationToken cancellationToken)
        {
            while (true)
            {
                ReadResult result = await reader.ReadAsync(cancellationToken);
                if (MqttPacketCodec.TryDecode(
                    result.Buffer,
                    out MqttPacket? packet,
                    out long consumed))
                {
                    reader.AdvanceTo(result.Buffer.GetPosition(consumed));
                    return Assert.IsType<MqttConnAckPacket>(packet);
                }

                reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
                if (result.IsCompleted)
                {
                    throw new EndOfStreamException();
                }
            }
        }

        private static async Task PublishQosOneToSubscriberAsync(
            MqttZeroBroker broker)
        {
            MqttZeroSessionState receiver =
                broker.CreateSessionState("background-receiver");
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "failure/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            AddSession(broker, receiver);
            broker.ResetRouteEpochFromSessions();
            PipeWriter writer = PipeWriter.Create(Stream.Null);
            await using MqttZeroClientConnection senderConnection = new(
                writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState sender =
                broker.CreateSessionState("background-sender");
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("failure/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>([1]),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            lease.PacketIdentifier = 7;
            try
            {
                await broker.RoutePublishLeaseAsync(
                    sender,
                    senderConnection,
                    lease,
                    TestContext.Current.CancellationToken);
            }
            finally
            {
                lease.Release();
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
                (ConcurrentDictionary<string, MqttZeroSessionState>)
                    sessionsField.GetValue(broker)!;
            sessions[session.ClientId] = session;
        }

        private static async Task WaitUntilAsync(
            Func<bool> predicate,
            CancellationToken cancellationToken)
        {
            using CancellationTokenSource timeout =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
            while (!predicate())
            {
                await Task.Delay(
                    TimeSpan.FromMilliseconds(10),
                    timeout.Token);
            }
        }

        private static async Task WaitForConnectedClientCountAsync(
            MqttZeroBroker broker,
            int expected,
            CancellationToken cancellationToken)
        {
            for (int attempt = 0; attempt < 40; attempt++)
            {
                if (broker.ConnectedClientCount == expected)
                {
                    return;
                }

                await Task.Delay(
                    TimeSpan.FromMilliseconds(50),
                    cancellationToken);
            }

            throw new TimeoutException(
                $"Connected client count did not reach {expected}.");
        }
    }
}
