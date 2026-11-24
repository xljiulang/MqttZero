using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Tests.Broker
{
    public sealed class InboundDispatcherTests
    {
        [Fact(Timeout = 5_000)]
        public async Task WorkFromOneConnectionRunsInOrderAndReleasesLeases()
        {
            List<int> order = [];
            TaskCompletionSource completed = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            MqttZeroInboundDispatcher dispatcher = new(
                2,
                8,
                (work, _) =>
                {
                    order.Add(work.Lease.PacketIdentifier);
                    if (order.Count == 3)
                    {
                        completed.TrySetResult();
                    }

                    return ValueTask.CompletedTask;
                },
                _ => { },
                TestContext.Current.CancellationToken);
            for (int index = 1; index <= 3; index++)
            {
                MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    new ReadOnlySequence<byte>("order/topic"u8.ToArray()),
                    ReadOnlySequence<byte>.Empty,
                    new ReadOnlySequence<byte>(new byte[8]),
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    false,
                    null,
                    null);
                lease.PacketIdentifier = (ushort)index;
                MqttZeroInboundWork work = MqttZeroInboundWork.Rent(
                    lease,
                    new MqttZeroSessionState("sender"),
                    null!);
                await dispatcher.EnqueueAsync(
                    42,
                    work,
                    TestContext.Current.CancellationToken);
            }

            await completed.Task.WaitAsync(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken);
            await dispatcher.StopAsync();
            Assert.Equal([1, 2, 3], order);
        }
    }
}
