using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Clients;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero.Tests.Broker
{
    public sealed class ClientWriteSerializationTests
    {
        [Fact(Timeout = 30_000)]
        public async Task ControlWriteClosesConnectionWhenTheGateStaysBusy()
        {
            BlockingFlushPipeWriter writer = new();
            await using MqttZeroClientConnection connection =
                CreateConnection(writer);
            List<MqttZeroQueuedMessage> messages = CreateMessages();
            using CancellationTokenSource cancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    TestContext.Current.CancellationToken);
            cancellation.CancelAfter(TimeSpan.FromSeconds(20));
            try
            {
                Task batch = Task.Run(
                    async () => await connection.SendPublishBatchAsync(
                        messages,
                        cancellation.Token),
                    cancellation.Token);
                await writer.FlushStarted.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    cancellation.Token);
                Assert.False(batch.IsCompleted);

                Task acknowledgement = Task.Run(
                    async () => await connection.SendPubAckAsync(
                        1,
                        MqttReasonCode.Success,
                        cancellation.Token),
                    cancellation.Token);
                await acknowledgement.WaitAsync(
                    TimeSpan.FromSeconds(8),
                    cancellation.Token);

                Assert.True(connection.IsClosed);
                writer.Release();
                await batch.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    cancellation.Token);
            }
            finally
            {
                writer.Release();
                ReleaseMessages(messages);
            }
        }

        [Fact(Timeout = 30_000)]
        public async Task ControlPacketAndPublishBatchWritesNeverOverlap()
        {
            OverlapDetectingPipeWriter writer = new(
                TimeSpan.FromMilliseconds(2));
            await using MqttZeroClientConnection connection =
                CreateConnection(writer);
            List<MqttZeroQueuedMessage> messages = CreateMessages();
            using CancellationTokenSource cancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    TestContext.Current.CancellationToken);
            cancellation.CancelAfter(TimeSpan.FromSeconds(20));
            try
            {
                Task control = Task.Run(
                    async () =>
                    {
                        for (int index = 0; index < 20; index++)
                        {
                            await connection.SendAsync(
                                new MqttPingRespPacket(),
                                cancellation.Token);
                        }
                    },
                    cancellation.Token);
                Task publish = Task.Run(
                    async () =>
                    {
                        for (int index = 0; index < 20; index++)
                        {
                            int encodedBytes =
                                await connection.EncodePublishBatchAsync(
                                    messages,
                                    cancellation.Token);
                            Assert.True(encodedBytes > 0);
                            await connection.FlushOutputAsync(
                                cancellation.Token);
                        }
                    },
                    cancellation.Token);

                await Task.WhenAll(control, publish);

                Assert.Equal(1, writer.MaxConcurrency);
                Assert.Equal(0, writer.OverlapCount);
            }
            finally
            {
                ReleaseMessages(messages);
            }
        }

        [Fact(Timeout = 30_000)]
        public async Task AcknowledgementAndPublishBatchWritesNeverOverlap()
        {
            OverlapDetectingPipeWriter writer = new(
                TimeSpan.FromMilliseconds(2));
            await using MqttZeroClientConnection connection =
                CreateConnection(writer);
            List<MqttZeroQueuedMessage> messages = CreateMessages();
            using CancellationTokenSource cancellation =
                CancellationTokenSource.CreateLinkedTokenSource(
                    TestContext.Current.CancellationToken);
            cancellation.CancelAfter(TimeSpan.FromSeconds(20));
            try
            {
                Task acknowledgement = Task.Run(
                    async () =>
                    {
                        for (int index = 0; index < 20; index++)
                        {
                            await connection.SendPubAckAsync(
                                (ushort)(index + 1),
                                MqttReasonCode.Success,
                                cancellation.Token);
                        }
                    },
                    cancellation.Token);
                Task publish = Task.Run(
                    async () =>
                    {
                        for (int index = 0; index < 20; index++)
                        {
                            int encodedBytes =
                                await connection.EncodePublishBatchAsync(
                                    messages,
                                    cancellation.Token);
                            Assert.True(encodedBytes > 0);
                            await connection.FlushOutputAsync(
                                cancellation.Token);
                        }
                    },
                    cancellation.Token);

                await Task.WhenAll(acknowledgement, publish);

                Assert.Equal(1, writer.MaxConcurrency);
                Assert.Equal(0, writer.OverlapCount);
            }
            finally
            {
                ReleaseMessages(messages);
            }
        }

        private static MqttZeroClientConnection CreateConnection(
            PipeWriter writer)
        {
            return new MqttZeroClientConnection(
                writer,
                remoteEndPoint: null,
                isSecure: false,
                isWebSocket: false,
                keepAliveSeconds: 0,
                receiveMaximum: 32,
                maximumPacketSize: 1024 * 1024,
                timeProvider: TimeProvider.System,
                allowPacketFragmentation: true);
        }

        private static List<MqttZeroQueuedMessage> CreateMessages()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("write/gate"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[1024]),
                MqttQualityOfServiceLevel.AtMostOnce,
                retain: false,
                messageExpiryInterval: null,
                subscriptionIdentifier: null);
            MqttZeroQueuedMessage message = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtMostOnce,
                retain: false,
                subscriptionIdentifier: null,
                expiresAt: null);
            lease.Release();
            return [message];
        }

        private static void ReleaseMessages(
            List<MqttZeroQueuedMessage> messages)
        {
            for (int index = 0; index < messages.Count; index++)
            {
                messages[index].Release();
            }
        }

        private sealed class BlockingFlushPipeWriter : PipeWriter
        {
            private readonly ArrayBufferWriter<byte> _buffer = new(
                1024 * 1024);
            private readonly TaskCompletionSource _flushStarted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly TaskCompletionSource _release = new(
                TaskCreationOptions.RunContinuationsAsynchronously);

            public Task FlushStarted
            {
                get
                {
                    return _flushStarted.Task;
                }
            }

            public void Release()
            {
                _release.TrySetResult();
            }

            public override void Advance(int bytes)
            {
                _buffer.Advance(bytes);
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

            public override async ValueTask<FlushResult> FlushAsync(
                CancellationToken cancellationToken = default)
            {
                _flushStarted.TrySetResult();
                await _release.Task
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
                return new FlushResult(false, false);
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                return _buffer.GetMemory(sizeHint);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return _buffer.GetSpan(sizeHint);
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                source.Span.CopyTo(_buffer.GetSpan(source.Length));
                _buffer.Advance(source.Length);
                return ValueTask.FromResult(new FlushResult(false, false));
            }
        }

        private sealed class OverlapDetectingPipeWriter : PipeWriter
        {
            private readonly ArrayBufferWriter<byte> _buffer = new(
                1024 * 1024);
            private readonly TimeSpan _delay;
            private readonly object _gate = new();
            private int _active;
            private int _maxConcurrency;
            private int _overlapCount;

            public OverlapDetectingPipeWriter(TimeSpan delay)
            {
                _delay = delay;
            }

            public int MaxConcurrency
            {
                get
                {
                    lock (_gate)
                    {
                        return _maxConcurrency;
                    }
                }
            }

            public int OverlapCount
            {
                get
                {
                    lock (_gate)
                    {
                        return _overlapCount;
                    }
                }
            }

            public override void Advance(int bytes)
            {
                _buffer.Advance(bytes);
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
                Enter();
                try
                {
                    Thread.Sleep(_delay);
                    return ValueTask.FromResult(
                        new FlushResult(false, false));
                }
                finally
                {
                    Exit();
                }
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                Enter();
                try
                {
                    return _buffer.GetMemory(sizeHint);
                }
                finally
                {
                    Exit();
                }
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                Enter();
                try
                {
                    return _buffer.GetSpan(sizeHint);
                }
                finally
                {
                    Exit();
                }
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                Enter();
                try
                {
                    source.Span.CopyTo(_buffer.GetSpan(source.Length));
                    _buffer.Advance(source.Length);
                    return ValueTask.FromResult(
                        new FlushResult(false, false));
                }
                finally
                {
                    Exit();
                }
            }

            private void Enter()
            {
                int active = Interlocked.Increment(ref _active);
                lock (_gate)
                {
                    _maxConcurrency = Math.Max(_maxConcurrency, active);
                    if (active > 1)
                    {
                        _overlapCount++;
                    }
                }
            }

            private void Exit()
            {
                Interlocked.Decrement(ref _active);
            }
        }
    }
}
