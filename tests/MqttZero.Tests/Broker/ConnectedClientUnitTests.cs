using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Clients;
using MqttZero.Server.Egress;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero.Tests.Broker
{
    public sealed class ConnectedClientUnitTests
    {
        [Fact]
        public async Task SubscribeAndUnsubscribeReturnPerFilterResults()
        {
            MqttZeroBroker broker = CreateBroker(new MqttZeroOptions());
            (MqttZeroSessionState session, MqttZeroClientConnection connection) =
                CreateConnection(broker, "unit");
            await using MqttZeroBroker brokerLifetime = broker;
            await using MqttZeroClientConnection connectionLifetime = connection;

            IReadOnlyList<MqttZeroSubscribeResult> subscribed =
                await connection.ConnectedClient.SubscribeAsync(
                    [
                        CreateSubscription("a/#", MqttQualityOfServiceLevel.AtMostOnce),
                        CreateSubscription("b/#", MqttQualityOfServiceLevel.AtLeastOnce),
                        CreateSubscription("c/#", MqttQualityOfServiceLevel.ExactlyOnce)
                    ],
                    subscriptionIdentifier: 9,
                    cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(
                [
                    MqttReasonCode.Success,
                    MqttReasonCode.GrantedQos1,
                    MqttReasonCode.GrantedQos2
                ],
                subscribed.Select(static result => result.ReasonCode));

            IReadOnlyList<MqttZeroUnsubscribeResult> unsubscribed =
                await connection.ConnectedClient.UnsubscribeAsync(
                    ["a/#", "missing/#"],
                    TestContext.Current.CancellationToken);

            Assert.Equal(
                [MqttReasonCode.Success, MqttReasonCode.NoSubscriptionExisted],
                unsubscribed.Select(static result => result.ReasonCode));
            Assert.Equal(2, session.Subscriptions.Count);
        }

        [Fact]
        public async Task SubscribeHonorsQuotaAndConsumerInterceptor()
        {
            MqttZeroBroker broker = CreateBroker(
                new MqttZeroOptions
                {
                    MaxSubscriptionsPerSession = 1
                },
                new ConsumingSubscribeInterceptor());
            (_, MqttZeroClientConnection connection) = CreateConnection(broker, "unit");
            await using MqttZeroBroker brokerLifetime = broker;
            await using MqttZeroClientConnection connectionLifetime = connection;

            IReadOnlyList<MqttZeroSubscribeResult> consumed =
                await connection.ConnectedClient.SubscribeAsync(
                    [CreateSubscription("denied/#", MqttQualityOfServiceLevel.AtLeastOnce)],
                    cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(
                MqttReasonCode.NotAuthorized,
                Assert.Single(consumed).ReasonCode);

            MqttZeroBroker allowedBroker = CreateBroker(
                new MqttZeroOptions
                {
                    MaxSubscriptionsPerSession = 1
                });
            (_, MqttZeroClientConnection allowedConnection) =
                CreateConnection(allowedBroker, "quota");
            await using MqttZeroBroker allowedLifetime = allowedBroker;
            await using MqttZeroClientConnection allowedConnectionLifetime =
                allowedConnection;
            _ = await allowedConnection.ConnectedClient.SubscribeAsync(
                [CreateSubscription("first/#")],
                cancellationToken: TestContext.Current.CancellationToken);
            IReadOnlyList<MqttZeroSubscribeResult> quota =
                await allowedConnection.ConnectedClient.SubscribeAsync(
                    [CreateSubscription("second/#")],
                    cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(MqttReasonCode.QuotaExceeded, Assert.Single(quota).ReasonCode);
        }

        [Fact]
        public async Task CloseConnectionInterceptorClosesHandle()
        {
            MqttZeroBroker broker = CreateBroker(
                new MqttZeroOptions(),
                new CloseSubscribeInterceptor());
            (_, MqttZeroClientConnection connection) = CreateConnection(broker, "close");
            await using MqttZeroBroker brokerLifetime = broker;
            await using MqttZeroClientConnection connectionLifetime = connection;

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await connection.ConnectedClient.SubscribeAsync(
                    [CreateSubscription("close/#")],
                    cancellationToken: TestContext.Current.CancellationToken));

            Assert.True(connection.ClosedToken.IsCancellationRequested);
        }

        [Fact]
        public async Task SubscriptionReplaysRetainedMessageOnlyForNewSubscription()
        {
            MqttZeroBroker broker = CreateBroker(new MqttZeroOptions());
            await using MqttZeroBroker brokerLifetime = broker;
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.PublishAsync(
                new MqttZeroPublishMessage
                {
                    Topic = "retained/unit",
                    Payload = new ReadOnlySequence<byte>("retained"u8.ToArray()),
                    QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                    Retain = true
                },
                TestContext.Current.CancellationToken);
            (MqttZeroSessionState session, MqttZeroClientConnection connection) = CreateConnection(
                broker,
                "retained");
            await using MqttZeroClientConnection connectionLifetime = connection;
            await using MqttZeroEgressActor pump =
                broker.StartEgress(
                    session,
                    connection,
                    TestContext.Current.CancellationToken);
            MqttSubscriptionRequest request = CreateSubscription(
                "retained/#",
                MqttQualityOfServiceLevel.ExactlyOnce);
            request.RetainHandling = MqttRetainHandling.SendAtSubscribe;

            _ = await connection.ConnectedClient.SubscribeAsync(
                [request],
                cancellationToken: TestContext.Current.CancellationToken);
            long messagesAfterFirst = await WaitForMessagesSentAsync(
                connection,
                1,
                TestContext.Current.CancellationToken);
            _ = await connection.ConnectedClient.SubscribeAsync(
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = request.TopicFilter,
                        QualityOfServiceLevel = request.QualityOfServiceLevel,
                        RetainHandling =
                            MqttRetainHandling.SendAtSubscribeIfNewSubscriptionOnly
                    }
                ],
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(1, messagesAfterFirst);
            Assert.Equal(0, session.PendingMessages.Count);
            Assert.Equal(messagesAfterFirst, connection.GetStatistics().MessagesSent);
        }

        [Fact]
        public async Task UnsubscribeInterceptorCanConsumeAndClose()
        {
            MqttZeroBroker consumedBroker = CreateBroker(
                new MqttZeroOptions(),
                unsubscribeInterceptor: new ConsumingUnsubscribeInterceptor());
            (_, MqttZeroClientConnection consumedConnection) = CreateConnection(
                consumedBroker,
                "consume-unsubscribe");
            await using MqttZeroBroker consumedLifetime = consumedBroker;
            await using MqttZeroClientConnection consumedConnectionLifetime =
                consumedConnection;
            _ = await consumedConnection.ConnectedClient.SubscribeAsync(
                [CreateSubscription("kept/#")],
                cancellationToken: TestContext.Current.CancellationToken);
            IReadOnlyList<MqttZeroUnsubscribeResult> consumed =
                await consumedConnection.ConnectedClient.UnsubscribeAsync(
                    ["kept/#"],
                    TestContext.Current.CancellationToken);
            Assert.Equal(MqttReasonCode.NotAuthorized, Assert.Single(consumed).ReasonCode);

            MqttZeroBroker closeBroker = CreateBroker(
                new MqttZeroOptions(),
                unsubscribeInterceptor: new CloseUnsubscribeInterceptor());
            (_, MqttZeroClientConnection closeConnection) = CreateConnection(
                closeBroker,
                "close-unsubscribe");
            await using MqttZeroBroker closeLifetime = closeBroker;
            await using MqttZeroClientConnection closeConnectionLifetime =
                closeConnection;
            _ = await closeConnection.ConnectedClient.SubscribeAsync(
                [CreateSubscription("close/#")],
                cancellationToken: TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await closeConnection.ConnectedClient.UnsubscribeAsync(
                    ["close/#"],
                    TestContext.Current.CancellationToken));
            Assert.True(closeConnection.ClosedToken.IsCancellationRequested);
        }

        [Fact]
        public async Task UnsubscribeRejectsInvalidFilterBeforeMutation()
        {
            MqttZeroBroker broker = CreateBroker(new MqttZeroOptions());
            (MqttZeroSessionState session, MqttZeroClientConnection connection) =
                CreateConnection(broker, "invalid-unsubscribe");
            await using MqttZeroBroker brokerLifetime = broker;
            await using MqttZeroClientConnection connectionLifetime = connection;
            _ = await connection.ConnectedClient.SubscribeAsync(
                [CreateSubscription("kept/#")],
                cancellationToken: TestContext.Current.CancellationToken);

            IReadOnlyList<MqttZeroUnsubscribeResult> results =
                await connection.ConnectedClient.UnsubscribeAsync(
                    ["invalid/#/filter"],
                    TestContext.Current.CancellationToken);

            Assert.Equal(MqttReasonCode.TopicFilterInvalid, Assert.Single(results).ReasonCode);
            Assert.Single(session.Subscriptions);
        }

        [Fact]
        public async Task SendPublishReturnsFalseWhenBrokerIsNotRunning()
        {
            MqttZeroBroker broker = CreateBroker(new MqttZeroOptions());
            (_, MqttZeroClientConnection connection) = CreateConnection(
                broker,
                "not-running");
            await using MqttZeroBroker brokerLifetime = broker;
            await using MqttZeroClientConnection connectionLifetime = connection;

            bool sent = await connection.ConnectedClient.SendPublishAsync(
                new MqttZeroPublishMessage
                {
                    Topic = "state/topic",
                    Payload = new ReadOnlySequence<byte>("value"u8.ToArray())
                },
                TestContext.Current.CancellationToken);

            Assert.False(sent);
        }

        [Fact(Timeout = 5_000)]
        public async Task ConnectedClientApiRunsOnSessionShard()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    EventLoopShardCount = 2
                },
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await using MqttZeroBroker brokerLifetime = broker;
            MqttZeroSessionState session =
                broker.CreateSessionState("sharded-api");
            MqttZeroClientConnection connection = new(
                new TrackingPipeWriter(),
                null,
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System,
                shardId: session.ShardId);
            await using MqttZeroClientConnection connectionLifetime =
                connection;
            connection.ClientInfo = new MqttZeroClientConnectionInfo
            {
                ClientId = session.ClientId
            };
            session.Connection = connection;
            connection.ConnectedClient = new MqttZeroConnectedClient(
                broker,
                session,
                connection);

            _ = await connection.ConnectedClient.SubscribeAsync(
                [CreateSubscription("sharded/#")],
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.Single(session.Subscriptions);
        }

        [Fact(Timeout = 5_000)]
        public async Task ConnectedClientApiCanBeCalledFromOwningShard()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    EventLoopShardCount = 2
                },
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await using MqttZeroBroker brokerLifetime = broker;
            MqttZeroSessionState session =
                broker.CreateSessionState("sharded-reentrant");
            MqttZeroClientConnection connection = new(
                new TrackingPipeWriter(),
                null,
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System,
                shardId: session.ShardId);
            await using MqttZeroClientConnection connectionLifetime =
                connection;
            connection.ClientInfo = new MqttZeroClientConnectionInfo
            {
                ClientId = session.ClientId
            };
            session.Connection = connection;
            connection.ConnectedClient = new MqttZeroConnectedClient(
                broker,
                session,
                connection);
            TaskCompletionSource completed = new(
                TaskCreationOptions.RunContinuationsAsynchronously);

            await broker.Shards![session.ShardId].EnqueueAsync(
                new DelegateShardWork(
                    async cancellationToken =>
                    {
                        _ = await connection.ConnectedClient.SubscribeAsync(
                            [CreateSubscription("reentrant/#")],
                            cancellationToken: cancellationToken);
                        completed.TrySetResult();
                    }),
                TestContext.Current.CancellationToken);

            await completed.Task.WaitAsync(
                TestContext.Current.CancellationToken);
            Assert.Single(session.Subscriptions);
        }

        private static MqttZeroBroker CreateBroker(
            MqttZeroOptions options,
            IMqttZeroSubscribeInterceptor? subscribeInterceptor = null,
            IMqttZeroUnsubscribeInterceptor? unsubscribeInterceptor = null)
        {
            return new MqttZeroBroker(
                Options.Create(options),
                TimeProvider.System,
                NullLogger<MqttZeroBroker>.Instance,
                [],
                [],
                subscribeInterceptor == null ? [] : [subscribeInterceptor],
                unsubscribeInterceptor == null ? [] : [unsubscribeInterceptor],
                [],
                [],
                [],
                new MqttZeroNoopSessionStore(),
                new MqttZeroNoopMessageStore(),
                new MqttZeroNoopRetainedMessageStore());
        }

        private static (
            MqttZeroSessionState Session,
            MqttZeroClientConnection Connection) CreateConnection(
            MqttZeroBroker broker,
            string clientId)
        {
            MqttZeroClientConnection connection = new(
                new TrackingPipeWriter(),
                null,
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            connection.ClientInfo = new MqttZeroClientConnectionInfo
            {
                ClientId = clientId
            };
            MqttZeroSessionState session = new(clientId);
            session.Connection = connection;
            connection.ConnectedClient = new MqttZeroConnectedClient(
                broker,
                session,
                connection);
            return (session, connection);
        }

        private static MqttSubscriptionRequest CreateSubscription(
            string filter,
            MqttQualityOfServiceLevel qos = MqttQualityOfServiceLevel.AtMostOnce)
        {
            return new MqttSubscriptionRequest
            {
                TopicFilter = new ReadOnlySequence<byte>(
                    System.Text.Encoding.UTF8.GetBytes(filter)),
                QualityOfServiceLevel = qos
            };
        }

        private static async Task<long> WaitForMessagesSentAsync(
            MqttZeroClientConnection connection,
            long expected,
            CancellationToken cancellationToken)
        {
            for (int attempt = 0; attempt < 100; attempt++)
            {
                long actual = connection.GetStatistics().MessagesSent;
                if (actual == expected)
                {
                    return actual;
                }

                await Task.Delay(
                    TimeSpan.FromMilliseconds(10),
                    cancellationToken);
            }

            throw new TimeoutException(
                $"Expected {expected} sent messages.");
        }

        private sealed class DelegateShardWork :
            MqttZero.Server.Scheduling.MqttZeroShardWork
        {
            private readonly Func<CancellationToken, ValueTask> _execute;

            public DelegateShardWork(
                Func<CancellationToken, ValueTask> execute)
            {
                _execute = execute;
            }

            public override ValueTask ExecuteAsync(
                MqttZero.Server.Scheduling.MqttZeroEventLoop shard,
                CancellationToken cancellationToken)
            {
                return _execute(cancellationToken);
            }
        }

        private sealed class ConsumingSubscribeInterceptor :
            IMqttZeroSubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroSubscribeContext context,
                CancellationToken cancellationToken)
            {
                context.ReasonCode = MqttReasonCode.NotAuthorized;
                return ValueTask.FromResult(MqttZeroInterceptionResult.Consume);
            }
        }

        private sealed class CloseSubscribeInterceptor :
            IMqttZeroSubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroSubscribeContext context,
                CancellationToken cancellationToken)
            {
                context.ReasonCode = MqttReasonCode.NotAuthorized;
                return ValueTask.FromResult(
                    MqttZeroInterceptionResult.CloseConnection);
            }
        }

        private sealed class ConsumingUnsubscribeInterceptor :
            IMqttZeroUnsubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroUnsubscribeContext context,
                CancellationToken cancellationToken)
            {
                context.ReasonCode = MqttReasonCode.NotAuthorized;
                return ValueTask.FromResult(MqttZeroInterceptionResult.Consume);
            }
        }

        private sealed class CloseUnsubscribeInterceptor :
            IMqttZeroUnsubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroUnsubscribeContext context,
                CancellationToken cancellationToken)
            {
                context.ReasonCode = MqttReasonCode.NotAuthorized;
                return ValueTask.FromResult(
                    MqttZeroInterceptionResult.CloseConnection);
            }
        }

        private sealed class TrackingPipeWriter : PipeWriter
        {
            private byte[] _buffer = new byte[256];
            private int _written;

            public override void Advance(int bytes)
            {
                _written += bytes;
            }

            public override void CancelPendingFlush()
            {
            }

            public override void Complete(Exception? exception = null)
            {
            }

            public override ValueTask CompleteAsync(Exception? exception = null)
            {
                return ValueTask.CompletedTask;
            }

            public override ValueTask<FlushResult> FlushAsync(
                CancellationToken cancellationToken = default)
            {
                return ValueTask.FromResult(new FlushResult(false, false));
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                EnsureCapacity(sizeHint);
                return _buffer.AsMemory(_written);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                EnsureCapacity(sizeHint);
                return _buffer.AsSpan(_written);
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                EnsureCapacity(source.Length);
                source.Span.CopyTo(_buffer.AsSpan(_written));
                _written += source.Length;
                return ValueTask.FromResult(new FlushResult(false, false));
            }

            private void EnsureCapacity(int sizeHint)
            {
                int required = _written + Math.Max(sizeHint, 1);
                if (required > _buffer.Length)
                {
                    Array.Resize(ref _buffer, Math.Max(required, _buffer.Length * 2));
                }
            }
        }
    }
}
