using MqttQualityOfServiceLevel = MqttZero.Protocol.MqttQualityOfServiceLevel;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Retained;
using MqttZero.Protocol;
using MqttZero.Publishing;
using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero.Tests.Broker
{
    public sealed class PublishApiTests
    {
        [Fact]
        public async Task PublishMessageEntityUsesTheSharedPublishPipeline()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            try
            {
                MqttZeroPublishMessage message = new()
                {
                    Topic = "api/entity",
                    Payload = new ReadOnlySequence<byte>("entity"u8.ToArray()),
                    QualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce,
                    Retain = true
                };

                await broker.PublishAsync(
                    message,
                    TestContext.Current.CancellationToken);

                MqttZeroRetainedMessageSnapshot retained =
                    Assert.Single(broker.GetRetainedMessageSnapshots());
                Assert.Equal("api/entity", retained.Topic);
                Assert.Equal(6, retained.PayloadLength);
            }
            finally
            {
                await broker.DisposeAsync();
            }
        }

        [Fact]
        public async Task PayloadFactoryExtensionBuildsAMessageForTheCoreMethod()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            try
            {
                await broker.PublishAsync(
                    "api/factory",
                    WritePayloadAsync,
                    MqttQualityOfServiceLevel.AtMostOnce,
                    true,
                    TestContext.Current.CancellationToken);

                MqttZeroRetainedMessageSnapshot retained =
                    Assert.Single(broker.GetRetainedMessageSnapshots());
                Assert.Equal("api/factory", retained.Topic);
                Assert.Equal(7, retained.PayloadLength);
            }
            finally
            {
                await broker.DisposeAsync();
            }

            static async ValueTask WritePayloadAsync(PipeWriter writer)
            {
                await writer.WriteAsync("factory"u8.ToArray());
                await writer.FlushAsync();
            }
        }

        [Fact]
        public async Task SequenceExtensionBuildsAMessageForTheCoreMethod()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            try
            {
                ReadOnlySequence<byte> payload =
                    new("sequence"u8.ToArray());
                await broker.PublishAsync(
                    "api/sequence",
                    payload,
                    MqttQualityOfServiceLevel.AtMostOnce,
                    true,
                    TestContext.Current.CancellationToken);

                MqttZeroRetainedMessageSnapshot retained =
                    Assert.Single(broker.GetRetainedMessageSnapshots());
                Assert.Equal("api/sequence", retained.Topic);
                Assert.Equal(8, retained.PayloadLength);
            }
            finally
            {
                await broker.DisposeAsync();
            }
        }

        [Fact]
        public async Task StreamExtensionDoesNotOwnOrDisposeTheCallerStream()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            TrackingReadStream payload = new("stream"u8.ToArray());
            try
            {
                await broker.PublishAsync(
                    "api/stream",
                    payload,
                    MqttQualityOfServiceLevel.AtMostOnce,
                    true,
                    TestContext.Current.CancellationToken);

                MqttZeroRetainedMessageSnapshot retained =
                    Assert.Single(broker.GetRetainedMessageSnapshots());
                Assert.Equal("api/stream", retained.Topic);
                Assert.Equal(6, retained.PayloadLength);
                Assert.False(payload.IsDisposed);
            }
            finally
            {
                await broker.DisposeAsync();
            }
        }

        [Fact]
        public async Task OversizedStreamPublishDoesNotRetainPartialPayload()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    MaxPacketSize = 32
                },
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            try
            {
                using MemoryStream payload = new(new byte[128]);

                await Assert.ThrowsAsync<InvalidOperationException>(
                    async () => await broker.PublishAsync(
                        "api/oversized",
                        payload,
                        MqttQualityOfServiceLevel.AtMostOnce,
                        true,
                        TestContext.Current.CancellationToken));

                Assert.Empty(broker.GetRetainedMessageSnapshots());
            }
            finally
            {
                await broker.DisposeAsync();
            }
        }

        [Fact]
        public async Task ServerPublishRejectsInvalidUtf8WhenPayloadFormatDeclared()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            try
            {
                await Assert.ThrowsAsync<ArgumentException>(
                    async () => await broker.PublishAsync(
                        new MqttZeroPublishMessage
                        {
                            Topic = "api/invalid-utf8",
                            Payload = new ReadOnlySequence<byte>([0xFF]),
                            Properties = new MqttPublishProperties
                            {
                                PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8
                            }
                        },
                        TestContext.Current.CancellationToken));
            }
            finally
            {
                await broker.DisposeAsync();
            }
        }

        [Fact]
        public async Task ZeroRetainedMessageLimitAllowsMultipleMessages()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions
                {
                    MaxRetainedMessages = 0
                },
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            try
            {
                foreach (string topic in new[] { "retained/one", "retained/two", "retained/three" })
                {
                    await broker.PublishAsync(
                        topic,
                        new ReadOnlySequence<byte>("value"u8.ToArray()),
                        MqttQualityOfServiceLevel.AtMostOnce,
                        true,
                        TestContext.Current.CancellationToken);
                }

                Assert.Equal(3, broker.GetRetainedMessageSnapshots().Count);
            }
            finally
            {
                await broker.DisposeAsync();
            }
        }

        private sealed class TrackingReadStream : MemoryStream
        {
            public TrackingReadStream(byte[] buffer)
                : base(buffer)
            {
            }

            public bool IsDisposed { get; private set; }

            protected override void Dispose(bool disposing)
            {
                IsDisposed = true;
                base.Dispose(disposing);
            }
        }
    }
}
