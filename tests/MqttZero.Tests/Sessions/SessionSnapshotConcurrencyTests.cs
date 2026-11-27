using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;
using MqttQualityOfServiceLevel =
    MqttZero.Protocol.MqttQualityOfServiceLevel;

namespace MqttZero.Tests.Sessions;

public sealed class SessionSnapshotConcurrencyTests
{
    [Fact(Timeout = 10_000)]
    public async Task LifecycleFieldsComeFromOnePublishedSnapshot()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        DateTimeOffset now = new(
            2026,
            11,
            26,
            12,
            0,
            0,
            TimeSpan.Zero);
        MqttZeroSessionState session = new("atomic-lifecycle");
        MqttZeroWillMessage firstWill = CreateWill("will/first");
        MqttZeroWillMessage secondWill = CreateWill("will/second");
        session.UpdateLifecycle(
            _ => new MqttZeroSessionLifecycleSnapshot(
                null,
                CleanStart: false,
                ExpiryInterval: 20,
                DisconnectedAt: now.AddSeconds(-5),
                WillMessage: secondWill,
                WillDueAt: now.AddSeconds(2)));
        Task writer = Task.Run(
            () =>
            {
                for (int index = 0; index < 5_000; index++)
                {
                    bool first = (index & 1) == 0;
                    session.UpdateLifecycle(
                        _ => new MqttZeroSessionLifecycleSnapshot(
                            null,
                            CleanStart: first,
                            ExpiryInterval: first ? 10U : 20U,
                            DisconnectedAt: first
                                ? now.AddSeconds(-10)
                                : now.AddSeconds(-5),
                            WillMessage: first
                                ? firstWill
                                : secondWill,
                            WillDueAt: first
                                ? now.AddSeconds(1)
                                : now.AddSeconds(2)));
                }
            },
            cancellationToken);
        int mismatches = 0;
        for (int index = 0; index < 5_000; index++)
        {
            MqttZeroSessionSnapshot snapshot =
                session.CreateSnapshot(now);
            bool valid = snapshot.CleanStart
                ? snapshot.ExpiryIntervalSeconds == 10 &&
                    snapshot.ExpiresAt == now &&
                    snapshot.WillTopic == "will/first"
                : snapshot.ExpiryIntervalSeconds == 20 &&
                    snapshot.ExpiresAt == now.AddSeconds(15) &&
                    snapshot.WillTopic == "will/second";
            if (!valid)
            {
                Interlocked.Increment(ref mismatches);
            }
        }

        await writer;
        Assert.Equal(0, mismatches);
    }

    [Fact(Timeout = 10_000)]
    public async Task SubscriptionReplacementPublishesStableArrays()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        MqttZeroSessionState session = new("atomic-subscriptions");
        MqttZeroSubscription first = CreateSubscription("first/#");
        MqttZeroSubscription second = CreateSubscription("second/#");
        session.Subscriptions.Replace([second]);
        Task writer = Task.Run(
            () =>
            {
                for (int index = 0; index < 5_000; index++)
                {
                    session.Subscriptions.Replace(
                        (index & 1) == 0
                            ? [first, second]
                            : [second]);
                }
            },
            cancellationToken);
        int invalidSnapshots = 0;
        for (int index = 0; index < 5_000; index++)
        {
            MqttZeroSubscription[] snapshot =
                session.Subscriptions.Snapshot();
            bool valid = snapshot.Length == 1
                ? ReferenceEquals(snapshot[0], second)
                : snapshot.Length == 2 &&
                    ReferenceEquals(snapshot[0], first) &&
                    ReferenceEquals(snapshot[1], second);
            if (!valid)
            {
                Interlocked.Increment(ref invalidSnapshots);
            }
        }

        await writer;
        Assert.Equal(0, invalidSnapshots);
    }

    [Fact(Timeout = 10_000)]
    public async Task ManagementReadsFromOwningShardDoNotSelfWait()
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
        Pipe pipe = new();
        await using MqttZeroClientConnection connection = new(
            pipe.Writer,
            null,
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        connection.ClientInfo = new MqttZeroClientConnectionInfo
        {
            ClientId = "shard-management"
        };
        MqttZeroSessionState session = new("shard-management");
        session.Connection = connection;
        ConcurrentDictionary<string, MqttZeroSessionState> sessions =
            GetSessionsDictionary(broker);
        sessions[session.ClientId] = session;

        bool completed = await broker.Shards![0].InvokeAsync(
            token =>
            {
                IReadOnlyList<MqttZeroSessionSnapshot> sessions =
                    broker.GetSessionSnapshots();
                IReadOnlyList<MqttZeroClientSnapshot> clients =
                    broker.GetClientSnapshots();
                _ = broker.TryGetClient(
                    session.ClientId,
                    out _);
                Assert.Single(sessions);
                Assert.Single(clients);
                token.ThrowIfCancellationRequested();
                return ValueTask.FromResult(true);
            },
            cancellationToken);

        Assert.True(completed);
    }

    [Fact(Timeout = 15_000)]
    public async Task RepeatedTakeoverAndManagementPollingRemainCoherent()
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
        Pipe firstPipe = new();
        Pipe secondPipe = new();
        await using MqttZeroClientConnection firstConnection = new(
            firstPipe.Writer,
            null,
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        await using MqttZeroClientConnection secondConnection = new(
            secondPipe.Writer,
            null,
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        firstConnection.ClientInfo = new MqttZeroClientConnectionInfo
        {
            ClientId = "takeover"
        };
        secondConnection.ClientInfo = new MqttZeroClientConnectionInfo
        {
            ClientId = "takeover"
        };
        MqttZeroSessionState session = new("takeover");
        firstConnection.ConnectedClient = new MqttZeroConnectedClient(
            broker,
            session,
            firstConnection);
        secondConnection.ConnectedClient = new MqttZeroConnectedClient(
            broker,
            session,
            secondConnection);
        session.Connection = firstConnection;
        GetSessionsDictionary(broker)[session.ClientId] = session;

        Task takeover = Task.Run(
            () =>
            {
                for (int index = 0; index < 1_000; index++)
                {
                    MqttZeroClientConnection connection =
                        (index & 1) == 0
                            ? firstConnection
                            : secondConnection;
                    session.UpdateLifecycle(
                        current => current with
                        {
                            Connection = connection,
                            CleanStart = (index & 2) == 0,
                            ExpiryInterval = (uint)(index + 1)
                        });
                }
            },
            cancellationToken);
        int invalidReads = 0;
        for (int index = 0; index < 1_000; index++)
        {
            MqttZeroSessionSnapshot sessionSnapshot =
                Assert.Single(broker.GetSessionSnapshots());
            if (!sessionSnapshot.IsConnected ||
                sessionSnapshot.SubscriptionCount != 0 ||
                !broker.TryGetClient("takeover", out _))
            {
                invalidReads++;
            }

            IReadOnlyList<MqttZeroClientSnapshot> clients =
                broker.GetClientSnapshots();
            if (clients.Count != 1 ||
                clients[0].ClientId != "takeover")
            {
                invalidReads++;
            }
        }

        await takeover;
        Assert.Equal(0, invalidReads);
    }

    [Fact(Timeout = 10_000)]
    public async Task ManagementSkipsDisposedConnectionWithoutDereferencingIt()
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
        Pipe pipe = new();
        MqttZeroClientConnection connection = new(
            pipe.Writer,
            null,
            false,
            false,
            0,
            32,
            1024 * 1024,
            TimeProvider.System);
        connection.ClientInfo = new MqttZeroClientConnectionInfo
        {
            ClientId = "disposed-management"
        };
        MqttZeroSessionState session = new("disposed-management");
        session.Connection = connection;
        GetSessionsDictionary(broker)[session.ClientId] = session;

        await connection.DisposeAsync();

        Assert.Empty(broker.GetClientSnapshots());
        Assert.False(broker.TryGetClient(
            session.ClientId,
            out _));
    }

    private static MqttZeroWillMessage CreateWill(string topic)
    {
        return new MqttZeroWillMessage(
            new MqttZero.Protocol.MqttConnectPacket
            {
                HasWill = true,
                WillTopic = new ReadOnlySequence<byte>(
                    System.Text.Encoding.UTF8.GetBytes(topic)),
                WillPayload = new ReadOnlySequence<byte>(
                    "will"u8.ToArray())
            });
    }

    private static MqttZeroSubscription CreateSubscription(
        string filter)
    {
        return new MqttZeroSubscription(
            System.Text.Encoding.UTF8.GetBytes(filter),
            MqttQualityOfServiceLevel.AtLeastOnce,
            false,
            false,
            null);
    }

    private static ConcurrentDictionary<
        string,
        MqttZeroSessionState> GetSessionsDictionary(
        MqttZeroBroker broker)
    {
        FieldInfo field = typeof(MqttZeroBroker).GetField(
            "_sessions",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (ConcurrentDictionary<
            string,
            MqttZeroSessionState>)field.GetValue(broker)!;
    }
}
