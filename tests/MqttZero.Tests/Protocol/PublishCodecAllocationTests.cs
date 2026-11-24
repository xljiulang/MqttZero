using MqttZero.Tests.Support;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;


namespace MqttZero.Tests.Protocol
{
    public sealed class PublishCodecAllocationTests
    {
        [Fact]
        public void PublishFastCodecDoesNotAllocateAfterWarmup()
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>(new byte[8192]),
                MessageExpiryInterval = 60,
                SubscriptionIdentifier = 7
            };
            ArrayBufferWriter<byte> setupWriter = new();
            MqttPacketCodec.Encode(packet, setupWriter);
            ReadOnlySequence<byte> source = new(setupWriter.WrittenMemory);
            PreallocatedBufferWriter writer = new(16 * 1024);

            for (int index = 0; index < 10; index++)
            {
                _ = MqttPublishPacketCodec.TryDecode(source, out MqttZeroPublishView warmup, out _);
                writer.Reset();
                MqttPublishPacketCodec.Encode(warmup, writer);
            }

            int totalPayload = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1_000; index++)
            {
                bool decoded = MqttPublishPacketCodec.TryDecode(
                    source,
                    out MqttZeroPublishView view,
                    out _);
                if (!decoded)
                {
                    throw new InvalidOperationException("The fast PUBLISH decoder failed.");
                }

                totalPayload += (int)view.Payload.Length;
                writer.Reset();
                MqttPublishPacketCodec.Encode(view, writer);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(8_192_000, totalPayload);
            Assert.Equal(0, allocated);
        }
        [Theory]
        [InlineData(8 * 1024, 1)]
        [InlineData(8 * 1024, 10)]
        [InlineData(64 * 1024, 1)]
        [InlineData(64 * 1024, 10)]
        public void FastPublishCodecHandlesSegmentedPayloadsWithoutAllocation(
            int payloadSize,
            int segmentCount)
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>(new byte[payloadSize]),
                MessageExpiryInterval = 60,
                SubscriptionIdentifier = 7
            };
            ArrayBufferWriter<byte> setupWriter = new();
            MqttPacketCodec.Encode(packet, setupWriter);
            ReadOnlySequence<byte> source = CreateSegmentedSequence(
                setupWriter.WrittenSpan.ToArray(),
                segmentCount);
            PreallocatedBufferWriter writer = new(payloadSize + 1024);

            for (int index = 0; index < 10; index++)
            {
                _ = MqttPublishPacketCodec.TryDecode(source, out MqttZeroPublishView warmup, out _);
                writer.Reset();
                MqttPublishPacketCodec.Encode(warmup, writer);
            }

            int totalPayload = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 100; index++)
            {
                bool decoded = MqttPublishPacketCodec.TryDecode(
                    source,
                    out MqttZeroPublishView view,
                    out _);
                if (!decoded)
                {
                    throw new InvalidOperationException("The segmented PUBLISH decoder failed.");
                }

                totalPayload += (int)view.Payload.Length;
                writer.Reset();
                MqttPublishPacketCodec.Encode(view, writer);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(payloadSize * 100, totalPayload);
            Assert.Equal(0, allocated);
        }

        [Fact]
        public void HeaderAndPayloadEncodingMatchesFullWriterEncoding()
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new System.Buffers.ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new System.Buffers.ReadOnlySequence<byte>(new byte[8192]),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                PacketIdentifier = 7,
                MessageExpiryInterval = 60,
                SubscriptionIdentifier = 3
            };
            System.Buffers.ArrayBufferWriter<byte> sourceWriter = new();
            MqttPacketCodec.Encode(packet, sourceWriter);
            System.Buffers.ReadOnlySequence<byte> source = new(sourceWriter.WrittenMemory);
            MqttPublishPacketCodec.TryDecode(source, out MqttZeroPublishView view, out _);

            MqttZeroPacketWriter headerWriter = new();
            MqttZeroPublishPacketBuffer buffer = MqttPublishPacketCodec.EncodeHeader(view, headerWriter);
            System.Buffers.ArrayBufferWriter<byte> materialized = new();
            buffer.Header.Span.CopyTo(materialized.GetSpan(buffer.Header.Length));
            materialized.Advance(buffer.Header.Length);
            foreach (System.ReadOnlyMemory<byte> segment in buffer.Payload)
            {
                segment.Span.CopyTo(materialized.GetSpan(segment.Length));
                materialized.Advance(segment.Length);
            }

            Assert.Equal(sourceWriter.WrittenSpan.ToArray(), materialized.WrittenSpan.ToArray());
        }

        [Fact]
        public void HeaderEncodingDoesNotAllocateAfterWarmup()
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new System.Buffers.ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new System.Buffers.ReadOnlySequence<byte>(new byte[8192]),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                PacketIdentifier = 7
            };
            System.Buffers.ArrayBufferWriter<byte> sourceWriter = new();
            MqttPacketCodec.Encode(packet, sourceWriter);
            System.Buffers.ReadOnlySequence<byte> source = new(sourceWriter.WrittenMemory);
            MqttPublishPacketCodec.TryDecode(source, out MqttZeroPublishView view, out _);
            MqttZeroPacketWriter writer = new();
            for (int index = 0; index < 10; index++)
            {
                _ = MqttPublishPacketCodec.EncodeHeader(view, writer);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                _ = MqttPublishPacketCodec.EncodeHeader(view, writer);
            }

            Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
        }

        [Fact]
        public void EncodesCorrelationDataLargerThanSignedShortMaximum()
        {
            byte[] properties = new byte[3 + 32_768];
            properties[0] = 0x09;
            properties[1] = 0x80;
            properties[2] = 0x00;
            MqttZeroPublishView view = new(
                new ReadOnlySequence<byte>("large/property"u8.ToArray()),
                CreateSegmentedSequence(properties, 10),
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                0,
                false,
                false,
                null,
                null,
                ReadOnlyMemory<uint>.Empty,
                null);
            ArrayBufferWriter<byte> writer = new();

            MqttPublishPacketCodec.Encode(view, writer);

            Assert.True(writer.WrittenCount > properties.Length);
        }

        [Fact]
        public void FindsMessageExpiryAfterLargeBinaryProperty()
        {
            byte[] properties = new byte[8 + 32_768];
            properties[0] = 0x09;
            properties[1] = 0x80;
            properties[2] = 0x00;
            properties[3 + 32_768] = 0x02;
            properties[4 + 32_768] = 0x00;
            properties[5 + 32_768] = 0x00;
            properties[6 + 32_768] = 0x00;
            properties[7 + 32_768] = 60;
            ReadOnlySequence<byte> propertySequence =
                CreateSegmentedSequence(properties, 10);

            Assert.True(
                MqttZeroPublishPropertyTransformer.ContainsProperty(
                    propertySequence,
                    0x02));
        }

        private static ReadOnlySequence<byte> CreateSegmentedSequence(
            byte[] bytes,
            int segmentCount)
        {
            if (segmentCount == 1)
            {
                return new ReadOnlySequence<byte>(bytes);
            }

            int segmentLength = (bytes.Length + segmentCount - 1) / segmentCount;
            SequenceSegment? first = null;
            SequenceSegment? last = null;
            for (int offset = 0; offset < bytes.Length; offset += segmentLength)
            {
                int length = Math.Min(segmentLength, bytes.Length - offset);
                SequenceSegment segment = new(bytes.AsMemory(offset, length));
                if (first == null)
                {
                    first = segment;
                }
                else
                {
                    last!.SetNext(segment);
                }

                last = segment;
            }

            return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
        }

    }
}
