using Microsoft.Extensions.DependencyInjection;
using MqttZero.Server;
using MqttZero.AspNetCore;
using MqttZero.Server.Broker;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using MqttZero.Memory;
using System.Buffers;
using System.Collections.Concurrent;
using System.Reflection;

namespace MqttZero.Tests.Storage
{
    public sealed class PersistenceStoreTests
    {
        [Fact]
        public async Task BrokerRestoresSessionsFromCustomStore()
        {
            TestSessionStore sessionStore = new();
            sessionStore.Sessions["restored"] = new MqttZeroPersistedSession
            {
                ClientId = "restored",
                ExpiryInterval = 60,
                Subscriptions =
                [
                    new MqttZeroPersistedSubscription
                    {
                        TopicFilter = "restored/#"u8.ToArray(),
                        QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                    }
                ]
            };
            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            services.AddSingleton<IMqttZeroMessageStore, TestMessageStore>();
            services.AddSingleton<IMqttZeroRetainedMessageStore, TestRetainedStore>();
            services.AddMqttZero();
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();

            await broker.StartAsync(TestContext.Current.CancellationToken);

            MqttZeroSessionSnapshot session = Assert.Single(broker.GetSessions());
            Assert.Equal("restored", session.ClientId);
            Assert.Equal(1, session.SubscriptionCount);
        }

        [Fact]
        public async Task RestoreEnforcesLimitsAndReturnsSkippedLeasesToPool()
        {
            TestSessionStore sessionStore = new();
            sessionStore.Sessions["limited"] = new MqttZeroPersistedSession
            {
                ClientId = "limited",
                ExpiryInterval = 60
            };
            TestMessageStore messageStore = new();
            messageStore.Messages.Add(CreateMessage(1, 1));
            messageStore.Messages.Add(CreateMessage(2, 2));
            TestRetainedStore retainedStore = new();
            retainedStore.Messages["retained/one"] = new MqttZeroPersistedRetainedMessage
            {
                Topic = "retained/one"u8.ToArray(),
                Payload = new byte[256]
            };
            retainedStore.Messages["retained/two"] = new MqttZeroPersistedRetainedMessage
            {
                Topic = "retained/two"u8.ToArray(),
                Payload = new byte[256]
            };
            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            services.AddSingleton<IMqttZeroRetainedMessageStore>(retainedStore);
            services.AddMqttZero(options =>
            {
                options.MaxInflight = 1;
                options.MaxMessageQueueLength = 1;
                options.MaxRetainedMessages = 1;
            });
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();
            MqttZeroBufferPool bufferPool = GetBufferPool(broker);

            await broker.StartAsync(TestContext.Current.CancellationToken);

            MqttZeroSessionSnapshot session = Assert.Single(broker.GetSessions());
            Assert.Equal(1, session.InflightMessageCount);
            Assert.Single(broker.GetRetainedMessages());
            Assert.Single(messageStore.DeletedMessageIds);
            Assert.True(bufferPool.RentedBytes > 0);

            await broker.DisposeAsync();
            Assert.Equal(0, bufferPool.RentedBytes);
        }

        [Fact]
        public async Task RestoreLimitsInboundQos2ByMaxAwaitingRel()
        {
            TestSessionStore sessionStore = new();
            sessionStore.Sessions["awaiting"] = new MqttZeroPersistedSession
            {
                ClientId = "awaiting",
                ExpiryInterval = 60
            };
            TestMessageStore messageStore = new();
            messageStore.Messages.Add(CreateInboundMessage(1));
            messageStore.Messages.Add(CreateInboundMessage(2));
            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            services.AddSingleton<IMqttZeroRetainedMessageStore, TestRetainedStore>();
            services.AddMqttZero(options =>
            {
                options.MaxInflight = 100;
                options.MaxAwaitingRel = 1;
            });
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();

            await broker.StartAsync(TestContext.Current.CancellationToken);

            MqttZeroSessionSnapshot session = Assert.Single(broker.GetSessions());
            Assert.Equal(1, session.InboundQos2MessageCount);
            Assert.Single(
                messageStore.Messages,
                message => message.Kind == MqttZeroPersistedMessageKind.InboundQos2);
        }

        [Fact]
        public async Task RestoreRoutesPersistedInboundDispatchAndDeletesRecord()
        {
            TestSessionStore sessionStore = new();
            sessionStore.Sessions["sender"] = new MqttZeroPersistedSession
            {
                ClientId = "sender",
                ExpiryInterval = 60
            };
            sessionStore.Sessions["receiver"] = new MqttZeroPersistedSession
            {
                ClientId = "receiver",
                ExpiryInterval = 60,
                Subscriptions =
                [
                    new MqttZeroPersistedSubscription
                    {
                        TopicFilter = "dispatch/#"u8.ToArray(),
                        QualityOfServiceLevel =
                            MqttQualityOfServiceLevel.AtLeastOnce
                    }
                ]
            };
            TestMessageStore messageStore = new();
            messageStore.Messages.Add(new MqttZeroPersistedMessage
            {
                ClientId = "sender",
                MessageId = 99,
                PacketIdentifier = 7,
                Kind = MqttZeroPersistedMessageKind.InboundDispatch,
                State = MqttZeroPersistedDeliveryState.Queued,
                Topic = "dispatch/value"u8.ToArray(),
                Payload = "payload"u8.ToArray(),
                QualityOfServiceLevel =
                    MqttQualityOfServiceLevel.AtLeastOnce
            });
            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            services.AddSingleton<IMqttZeroRetainedMessageStore, TestRetainedStore>();
            services.AddMqttZero();
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();

            await broker.StartAsync(TestContext.Current.CancellationToken);

            MqttZeroSessionSnapshot receiver = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "receiver");
            Assert.Equal(1, receiver.PendingMessageCount);
            Assert.DoesNotContain(
                messageStore.Messages,
                message => message.Kind ==
                    MqttZeroPersistedMessageKind.InboundDispatch);
        }

        [Fact]
        public async Task RestoreRoutesInboundDispatchesInMessageIdOrder()
        {
            TestSessionStore sessionStore = new();
            sessionStore.Sessions["sender"] = new MqttZeroPersistedSession
            {
                ClientId = "sender",
                ExpiryInterval = 60
            };
            sessionStore.Sessions["receiver"] = new MqttZeroPersistedSession
            {
                ClientId = "receiver",
                ExpiryInterval = 60,
                Subscriptions =
                [
                    new MqttZeroPersistedSubscription
                    {
                        TopicFilter = "ordered/#"u8.ToArray(),
                        QualityOfServiceLevel =
                            MqttQualityOfServiceLevel.AtLeastOnce
                    }
                ]
            };
            TestMessageStore messageStore = new();
            for (int index = 1; index <= 3; index++)
            {
                messageStore.Messages.Add(new MqttZeroPersistedMessage
                {
                    ClientId = "sender",
                    MessageId = index,
                    PacketIdentifier = (ushort)index,
                    Kind = MqttZeroPersistedMessageKind.InboundDispatch,
                    State = MqttZeroPersistedDeliveryState.Queued,
                    Topic = System.Text.Encoding.UTF8.GetBytes(
                        $"ordered/{index}"),
                    Payload = new[] { (byte)index },
                    QualityOfServiceLevel =
                        MqttQualityOfServiceLevel.AtLeastOnce
                });
            }

            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            services.AddSingleton<IMqttZeroRetainedMessageStore, TestRetainedStore>();
            services.AddMqttZero();
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();

            await broker.StartAsync(TestContext.Current.CancellationToken);

            MqttZeroSessionState receiver = GetSession(broker, "receiver");
            List<string> topics = [];
            foreach (MqttZeroQueuedMessage message in
                receiver.PendingMessages)
            {
                topics.Add(System.Text.Encoding.UTF8.GetString(
                    message.Topic.Span));
            }
            Assert.Equal(
                ["ordered/1", "ordered/2", "ordered/3"],
                topics);
        }

        [Fact]
        public async Task RestoreTreatsZeroRetainedCountAsUnlimited()
        {
            TestRetainedStore retainedStore = new();
            retainedStore.Messages["retained/one"] = new MqttZeroPersistedRetainedMessage
            {
                Topic = "retained/one"u8.ToArray(),
                Payload = "one"u8.ToArray()
            };
            retainedStore.Messages["retained/two"] = new MqttZeroPersistedRetainedMessage
            {
                Topic = "retained/two"u8.ToArray(),
                Payload = "two"u8.ToArray()
            };
            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore, TestSessionStore>();
            services.AddSingleton<IMqttZeroMessageStore, TestMessageStore>();
            services.AddSingleton<IMqttZeroRetainedMessageStore>(retainedStore);
            services.AddMqttZero(options => options.MaxRetainedMessages = 0);
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();

            await broker.StartAsync(TestContext.Current.CancellationToken);

            Assert.Equal(2, broker.GetRetainedMessages().Count);
        }

        [Fact]
        public async Task RestoreFailureLeavesBrokerEmptyAndReleasesStagedLeases()
        {
            TestSessionStore sessionStore = new();
            sessionStore.Sessions["broken"] = new MqttZeroPersistedSession
            {
                ClientId = "broken",
                ExpiryInterval = 60
            };
            TestMessageStore messageStore = new()
            {
                LoadException = new InvalidOperationException("load failed")
            };
            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            services.AddSingleton<IMqttZeroMessageStore>(messageStore);
            services.AddSingleton<IMqttZeroRetainedMessageStore, TestRetainedStore>();
            services.AddMqttZero();
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();
            MqttZeroBufferPool bufferPool = GetBufferPool(broker);

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await broker.StartAsync(
                    TestContext.Current.CancellationToken));

            Assert.Empty(broker.GetSessions());
            Assert.Empty(broker.GetRetainedMessages());
            Assert.Equal(0, bufferPool.RentedBytes);
        }

        [Fact]
        public async Task RetainedStoreFailureAndCapacityLimitsLeaveMemoryUnchanged()
        {
            TestRetainedStore retainedStore = new()
            {
                FailNextSave = true
            };
            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore, TestSessionStore>();
            services.AddSingleton<IMqttZeroMessageStore, TestMessageStore>();
            services.AddSingleton<IMqttZeroRetainedMessageStore>(retainedStore);
            services.AddMqttZero(options => options.MaxRetainedBytes = 20);
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();
            await broker.StartAsync(TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await broker.PublishAsync(
                    "retained/a",
                    new ReadOnlySequence<byte>(new byte[4]),
                    MqttQualityOfServiceLevel.AtMostOnce,
                    true,
                    TestContext.Current.CancellationToken));
            Assert.Empty(broker.GetRetainedMessages());
            Assert.Empty(retainedStore.Messages);

            await broker.PublishAsync(
                "retained/a",
                new ReadOnlySequence<byte>(new byte[4]),
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);
            Assert.Single(broker.GetRetainedMessages());
            Assert.Single(retainedStore.Messages);

            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await broker.PublishAsync(
                    "retained/b",
                    new ReadOnlySequence<byte>(new byte[4]),
                    MqttQualityOfServiceLevel.AtMostOnce,
                    true,
                    TestContext.Current.CancellationToken));
            Assert.Single(broker.GetRetainedMessages());
            Assert.Single(retainedStore.Messages);
            await broker.DisposeAsync();
        }

        private static MqttZeroPersistedMessage CreateMessage(
            long messageId,
            ushort packetIdentifier)
        {
            return new MqttZeroPersistedMessage
            {
                ClientId = "limited",
                MessageId = messageId,
                PacketIdentifier = packetIdentifier,
                Kind = MqttZeroPersistedMessageKind.Outbound,
                State = MqttZeroPersistedDeliveryState.AwaitPubAck,
                Topic = "limited/topic"u8.ToArray(),
                Payload = new byte[256],
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
            };
        }

        private static MqttZeroPersistedMessage CreateInboundMessage(
            ushort packetIdentifier)
        {
            return new MqttZeroPersistedMessage
            {
                ClientId = "awaiting",
                MessageId = packetIdentifier,
                PacketIdentifier = packetIdentifier,
                Kind = MqttZeroPersistedMessageKind.InboundQos2,
                State = MqttZeroPersistedDeliveryState.Queued,
                Topic = "awaiting/topic"u8.ToArray(),
                Payload = "value"u8.ToArray(),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.ExactlyOnce
            };
        }

        private static MqttZeroBufferPool GetBufferPool(MqttZeroBroker broker)
        {
            FieldInfo field = typeof(MqttZeroBroker).GetField(
                "_bufferPool",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            return (MqttZeroBufferPool)field.GetValue(broker)!;
        }

        private static MqttZeroSessionState GetSession(
            MqttZeroBroker broker,
            string clientId)
        {
            FieldInfo field = typeof(MqttZeroBroker).GetField(
                "_sessions",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            ConcurrentDictionary<string, MqttZeroSessionState> sessions =
                (ConcurrentDictionary<string, MqttZeroSessionState>)
                    field.GetValue(broker)!;
            return sessions[clientId];
        }

        private sealed class TestSessionStore : IMqttZeroSessionStore
        {
            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public Dictionary<string, MqttZeroPersistedSession> Sessions { get; } = [];

            public ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedSession>>(
                    Sessions.Values.ToArray());
            }

            public ValueTask SaveAsync(
                MqttZeroPersistedSession session,
                CancellationToken cancellationToken)
            {
                Sessions[session.ClientId] = session;
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteAsync(string clientId, CancellationToken cancellationToken)
            {
                Sessions.Remove(clientId);
                return ValueTask.CompletedTask;
            }
        }

        private sealed class TestMessageStore : IMqttZeroMessageStore
        {
            public List<MqttZeroPersistedMessage> Messages { get; } = [];

            public HashSet<long> DeletedMessageIds { get; } = [];

            public Exception? LoadException { get; init; }

            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedMessage>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                if (LoadException != null)
                {
                    throw LoadException;
                }

                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedMessage>>(
                    Messages.ToArray());
            }

            public ValueTask SaveQueuedAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
            {
                ReplaceMessage(message);
                return ValueTask.CompletedTask;
            }

            public ValueTask SaveOutboundAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
            {
                ReplaceMessage(message);
                return ValueTask.CompletedTask;
            }

            public ValueTask SaveInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
            {
                ReplaceMessage(message);
                return ValueTask.CompletedTask;
            }

            public ValueTask TransitionQueuedToOutboundAsync(
                string clientId,
                long messageId,
                MqttZeroPersistedMessage message,
                CancellationToken cancellationToken)
            {
                Messages.RemoveAll(item =>
                    item.ClientId == clientId &&
                    item.MessageId == messageId);
                Messages.Add(message);
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteMessageAsync(
                string clientId,
                long messageId,
                CancellationToken cancellationToken)
            {
                DeletedMessageIds.Add(messageId);
                Messages.RemoveAll(item =>
                    item.ClientId == clientId &&
                    item.MessageId == messageId);
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteInboundQos2Async(string clientId, ushort packetIdentifier, CancellationToken cancellationToken)
            {
                Messages.RemoveAll(item =>
                    item.ClientId == clientId &&
                    item.Kind == MqttZeroPersistedMessageKind.InboundQos2 &&
                    item.PacketIdentifier == packetIdentifier);
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteByClientAsync(
                string clientId,
                CancellationToken cancellationToken)
            {
                Messages.RemoveAll(item => item.ClientId == clientId);
                return ValueTask.CompletedTask;
            }

            public ValueTask ReplaceQueuedAsync(
                string clientId,
                long replacedMessageId,
                MqttZeroPersistedMessage message,
                CancellationToken cancellationToken)
            {
                Messages.RemoveAll(item =>
                    item.ClientId == clientId &&
                    item.MessageId == replacedMessageId);
                Messages.Add(message);
                return ValueTask.CompletedTask;
            }

            private void ReplaceMessage(MqttZeroPersistedMessage message)
            {
                Messages.RemoveAll(item =>
                    item.ClientId == message.ClientId &&
                    item.MessageId == message.MessageId);
                Messages.Add(message);
            }
        }

        private sealed class TestRetainedStore : IMqttZeroRetainedMessageStore
        {
            private int _failNextSave;

            public Dictionary<string, MqttZeroPersistedRetainedMessage> Messages { get; } = [];

            public bool FailNextSave
            {
                get
                {
                    return Volatile.Read(ref _failNextSave) != 0;
                }
                set
                {
                    Volatile.Write(ref _failNextSave, value ? 1 : 0);
                }
            }

            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedRetainedMessage>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedRetainedMessage>>(
                    Messages.Values.ToArray());
            }

            public ValueTask SaveAsync(MqttZeroPersistedRetainedMessage message, CancellationToken cancellationToken)
            {
                if (Interlocked.Exchange(ref _failNextSave, 0) != 0)
                {
                    throw new InvalidOperationException("Retained save failed.");
                }

                Messages[System.Text.Encoding.UTF8.GetString(message.Topic.Span)] = message;
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteAsync(string topic, CancellationToken cancellationToken)
            {
                Messages.Remove(topic);
                return ValueTask.CompletedTask;
            }

            public ValueTask ClearAsync(CancellationToken cancellationToken)
            {
                Messages.Clear();
                return ValueTask.CompletedTask;
            }
        }
    }
}
