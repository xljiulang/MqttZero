using MqttZero.Protocol;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.IO.Pipelines;

namespace MqttZero.Tests.Broker;

public sealed class AcknowledgementWorkPoolTests
{
    [Fact(Timeout = 15_000)]
    public async Task RentAndReleaseKeepBothPoolsBalanced()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        (MqttZeroSessionState session, MqttZeroClientConnection connection) =
            await CreateContextAsync();
        try
        {
            for (int iteration = 0; iteration < 1_000; iteration++)
            {
                await RentAndReleaseAsync(
                    session,
                    connection,
                    throwFault: false,
                    cancellationToken);
            }

            int steadyWork = MqttZeroAcknowledgementWork.PoolCountForTest;
            int steadyShardWork =
                MqttZeroAcknowledgementShardWork.PoolCountForTest;
            for (int iteration = 0; iteration < 1_000; iteration++)
            {
                await RentAndReleaseAsync(
                    session,
                    connection,
                    throwFault: false,
                    cancellationToken);
            }

            Assert.Equal(
                steadyWork,
                MqttZeroAcknowledgementWork.PoolCountForTest);
            Assert.Equal(
                steadyShardWork,
                MqttZeroAcknowledgementShardWork.PoolCountForTest);
            Assert.InRange(
                steadyWork,
                1,
                MqttZero.Memory.MqttZeroBoundedObjectPool<
                    MqttZeroAcknowledgementWork>.DefaultCapacity);
            Assert.InRange(
                steadyShardWork,
                1,
                MqttZero.Memory.MqttZeroBoundedObjectPool<
                    MqttZeroAcknowledgementShardWork>.DefaultCapacity);
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    [Fact(Timeout = 15_000)]
    public async Task FaultedProcessorStillReturnsBothWorks()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        (MqttZeroSessionState session, MqttZeroClientConnection connection) =
            await CreateContextAsync();
        try
        {
            int faults = 0;

            for (int iteration = 0; iteration < 100; iteration++)
            {
                faults += await RentAndReleaseAsync(
                    session,
                    connection,
                    throwFault: true,
                    cancellationToken);
            }

            int steadyWork = MqttZeroAcknowledgementWork.PoolCountForTest;
            int steadyShardWork =
                MqttZeroAcknowledgementShardWork.PoolCountForTest;
            for (int iteration = 0; iteration < 100; iteration++)
            {
                faults += await RentAndReleaseAsync(
                    session,
                    connection,
                    throwFault: true,
                    cancellationToken);
            }

            Assert.Equal(200, faults);
            Assert.Equal(
                steadyWork,
                MqttZeroAcknowledgementWork.PoolCountForTest);
            Assert.Equal(
                steadyShardWork,
                MqttZeroAcknowledgementShardWork.PoolCountForTest);
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }

    [Fact]
    public async Task BrokerSnapshotReportsTheAcknowledgementPools()
    {
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            new MqttZero.Server.MqttZeroOptions(),
            TimeProvider.System);
        await broker.StartAsync(TestContext.Current.CancellationToken);

        MqttZeroBrokerMemorySnapshot snapshot =
            broker.GetMemorySnapshotForTest();

        Assert.Equal(
            MqttZeroAcknowledgementWork.PoolCountForTest,
            snapshot.ObjectPools.AcknowledgementWorkCount);
        Assert.Equal(
            MqttZeroAcknowledgementShardWork.PoolCountForTest,
            snapshot.ObjectPools.AcknowledgementShardWorkCount);
    }

    private static async Task<int> RentAndReleaseAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        bool throwFault,
        CancellationToken cancellationToken)
    {
        int faults = 0;
        MqttZeroAcknowledgementWork work =
            MqttZeroAcknowledgementWork.Rent(
                session,
                connection,
                new MqttZeroAcknowledgementView(
                    MqttPacketType.PubAck,
                    1,
                    MqttReasonCode.Success),
                _ => { },
                new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously));
        MqttZeroAcknowledgementShardWork shardWork =
            MqttZeroAcknowledgementShardWork.Rent(
                work,
                (_, _) => throwFault
                    ? throw new InvalidOperationException("test fault")
                    : ValueTask.CompletedTask,
                _ => faults++);
        try
        {
            await shardWork.ExecuteAsync(null!, cancellationToken);
        }
        catch (InvalidOperationException)
        {
        }
        finally
        {
            shardWork.Release();
        }

        return faults;
    }

    private static async Task<(
        MqttZeroSessionState Session,
        MqttZeroClientConnection Connection)> CreateContextAsync()
    {
        MqttZeroSessionState session = new("pool-client");
        MqttZeroClientConnection connection = new(
            new DiscardingPipeWriter(),
            null,
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        await Task.CompletedTask;
        return (session, connection);
    }

    private sealed class DiscardingPipeWriter : PipeWriter
    {
        private byte[] _buffer = new byte[1024];

        public override void Advance(int bytes)
        {
        }

        public override void CancelPendingFlush()
        {
        }

        public override void Complete(Exception? exception = null)
        {
        }

        public override ValueTask<FlushResult> FlushAsync(
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(new FlushResult(false, false));
        }

        public override Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer;
        }

        public override Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer;
        }

        private void EnsureCapacity(int sizeHint)
        {
            int required = Math.Max(1, sizeHint);
            if (required > _buffer.Length)
            {
                _buffer = new byte[Math.Max(required, _buffer.Length * 2)];
            }
        }
    }
}
