using MqttZero.Protocol;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Scheduling;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;

namespace MqttZero.Tests.Broker;

public sealed class SessionShardOwnershipTests
{
    [Fact(Timeout = 10_000)]
    public async Task ManagementDisconnectWaitsForOwningShard()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        await using MqttZeroBroker broker = MqttZeroBroker.Create(
            new MqttZeroOptions
            {
                EventLoopShardCount = 1
            },
            TimeProvider.System);
        await broker.StartAsync(cancellationToken);
        OwnershipPipeWriter writer = new();
        await using MqttZeroClientConnection connection = new(
            writer,
            null,
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System,
            shardId: 0);
        connection.ClientInfo = new MqttZeroClientConnectionInfo
        {
            ClientId = "owned-disconnect"
        };
        MqttZeroSessionState session =
            broker.CreateSessionState("owned-disconnect");
        session.Connection = connection;
        connection.ConnectedClient = new MqttZeroConnectedClient(
            broker,
            session,
            connection);
        GetSessions(broker)[session.ClientId] = session;
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await broker.Shards![0].EnqueueAsync(
            new BlockingShardWork(
                blockerStarted,
                releaseBlocker),
            cancellationToken);
        await blockerStarted.Task.WaitAsync(cancellationToken);
        Task<bool> disconnectTask = broker.DisconnectClientAsync(
            session.ClientId,
            MqttReasonCode.AdministrativeAction,
            cancellationToken).AsTask();
        try
        {
            await Task.Delay(
                TimeSpan.FromMilliseconds(100),
                cancellationToken);
            Assert.False(disconnectTask.IsCompleted);
        }
        finally
        {
            releaseBlocker.TrySetResult();
        }

        Assert.True(await disconnectTask.WaitAsync(cancellationToken));
    }

    private static ConcurrentDictionary<string, MqttZeroSessionState>
        GetSessions(MqttZeroBroker broker)
    {
        FieldInfo field = typeof(MqttZeroBroker).GetField(
            "_sessions",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (ConcurrentDictionary<
            string,
            MqttZeroSessionState>)field.GetValue(broker)!;
    }

    private sealed class BlockingShardWork : MqttZeroShardWork
    {
        private readonly TaskCompletionSource _release;
        private readonly TaskCompletionSource _started;

        public BlockingShardWork(
            TaskCompletionSource started,
            TaskCompletionSource release)
        {
            _started = started;
            _release = release;
        }

        public override async ValueTask ExecuteAsync(
            MqttZeroEventLoop shard,
            CancellationToken cancellationToken)
        {
            _started.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
        }
    }

    private sealed class OwnershipPipeWriter : PipeWriter
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

        public override ValueTask CompleteAsync(
            Exception? exception = null)
        {
            return ValueTask.CompletedTask;
        }

        public override ValueTask<FlushResult> FlushAsync(
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult(
                new FlushResult(false, false));
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

        public override ValueTask<FlushResult> WriteAsync(
            ReadOnlyMemory<byte> source,
            CancellationToken cancellationToken = default)
        {
            EnsureCapacity(source.Length);
            return ValueTask.FromResult(
                new FlushResult(false, false));
        }

        private void EnsureCapacity(int sizeHint)
        {
            int required = Math.Max(1, sizeHint);
            if (required > _buffer.Length)
            {
                _buffer = new byte[Math.Max(
                    required,
                    _buffer.Length * 2)];
            }
        }
    }
}
