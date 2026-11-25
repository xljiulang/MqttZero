using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Tests.Protocol
{
    public sealed class MqttPublishPacketCodecBoundaryTests
    {
        [Fact]
        public void RejectsPropertyThatIsNotAllowedForPublish()
        {
            byte[] bytes =
            [
                0x30, 0x09,
                0x00, 0x01, (byte)'a',
                0x05, 0x11, 0x00, 0x00, 0x00, 0x01
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void EncodesRawPropertiesAndAppendsMissingScalars()
        {
            MqttZeroPublishView view = new(
                new ReadOnlySequence<byte>("properties/topic"u8.ToArray()),
                new ReadOnlySequence<byte>([0x01, 0x01]),
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                0,
                false,
                false,
                null,
                9,
                3);
            ArrayBufferWriter<byte> writer = new();

            MqttPublishPacketCodec.Encode(view, writer);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenMemory),
                out MqttPacket? packet,
                out _));
            MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(packet);
            Assert.Equal(9U, publish.SubscriptionIdentifier);
            Assert.Equal((ushort)3, publish.TopicAlias);
        }

        [Fact]
        public void EncodeToWriterUsesPreencodedHeaderForSmallPayload()
        {
            MqttZeroPublishView view = new(
                new ReadOnlySequence<byte>("small/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                7,
                true,
                true,
                null,
                null,
                null);
            ArrayBufferWriter<byte> writer = new();
            MqttZeroPacketWriter headerWriter = new();

            int encodedLength = MqttPublishPacketCodec.EncodeToWriter(
                view,
                headerWriter,
                writer);

            Assert.Equal(writer.WrittenCount, encodedLength);
            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenMemory),
                out MqttPacket? packet,
                out _));
            MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(packet);
            Assert.True(publish.Duplicate);
            Assert.True(publish.Retain);
        }

        [Fact]
        public void EncodeToWriterFragmentsLargePayloadSpanRequests()
        {
            MqttZeroPublishView view = new(
                new ReadOnlySequence<byte>("large/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[8 * 1024]),
                MqttQualityOfServiceLevel.AtMostOnce,
                0,
                false,
                false,
                null,
                null,
                null);
            RecordingBufferWriter writer = new(16 * 1024);
            MqttZeroPacketWriter headerWriter = new();

            _ = MqttPublishPacketCodec.EncodeToWriter(
                view,
                headerWriter,
                writer);

            Assert.InRange(writer.MaximumSizeHint, 1, 4096);
        }

        [Fact]
        public void EncodedLengthIncludesRewrittenExpiryAndScalars()
        {
            MqttZeroPublishView view = new(
                new ReadOnlySequence<byte>("length/topic"u8.ToArray()),
                new ReadOnlySequence<byte>(
                    [0x02, 0x00, 0x00, 0x00, 0x3C, 0x01, 0x01]),
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                0,
                false,
                false,
                30,
                5,
                null);

            int length = MqttPublishPacketCodec.GetEncodedLength(view);
            ArrayBufferWriter<byte> writer = new();
            MqttPublishPacketCodec.Encode(view, writer);

            Assert.Equal(writer.WrittenCount, length);
        }

        [Fact]
        public void FastCodecRoundTripsMultipleSubscriptionIdentifiers()
        {
            MqttZeroPublishView source = new(
                new ReadOnlySequence<byte>("multiple/ids"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                0,
                false,
                false,
                null,
                null,
                new uint[] { 7, 9 },
                null);
            ArrayBufferWriter<byte> writer = new();

            MqttPublishPacketCodec.Encode(source, writer);
            Assert.True(MqttPublishPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenMemory),
                out MqttZeroPublishView decoded,
                out _));

            Assert.Equal(new uint[] { 7, 9 }, decoded.SubscriptionIdentifiers.ToArray());
        }

        private sealed class RecordingBufferWriter : IBufferWriter<byte>
        {
            private readonly byte[] _buffer;
            private int _written;

            public RecordingBufferWriter(int capacity)
            {
                _buffer = new byte[capacity];
            }

            public int MaximumSizeHint { get; private set; }

            public void Advance(int count)
            {
                _written += count;
            }

            public Memory<byte> GetMemory(int sizeHint = 0)
            {
                RecordSizeHint(sizeHint);
                return _buffer.AsMemory(_written);
            }

            public Span<byte> GetSpan(int sizeHint = 0)
            {
                RecordSizeHint(sizeHint);
                return _buffer.AsSpan(_written);
            }

            private void RecordSizeHint(int sizeHint)
            {
                MaximumSizeHint = Math.Max(
                    MaximumSizeHint,
                    Math.Max(sizeHint, 1));
            }
        }
    }
}
