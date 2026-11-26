using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Tests.Protocol
{
    public sealed class MqttZeroWireFormatTests
    {
        [Theory]
        [InlineData(0, 1)]
        [InlineData(127, 1)]
        [InlineData(128, 2)]
        [InlineData(16_383, 2)]
        [InlineData(16_384, 3)]
        [InlineData(2_097_151, 3)]
        [InlineData(2_097_152, 4)]
        [InlineData(268_435_455, 4)]
        public void VariableByteIntegerRoundTripsAtBoundaries(int value, int expectedLength)
        {
            Assert.Equal(
                expectedLength,
                MqttZeroWireFormat.GetVariableByteIntegerSize(value));

            Span<byte> destination = stackalloc byte[4];
            int written = MqttZeroWireFormat.WriteVariableByteInteger(destination, value);
            Assert.Equal(expectedLength, written);

            byte[] encoded = destination[..written].ToArray();
            SequenceReader<byte> reader = new(new ReadOnlySequence<byte>(encoded));
            Assert.True(
                MqttZeroWireFormat.TryReadVariableByteInteger(
                    ref reader,
                    out int decoded));
            Assert.Equal(value, decoded);
            Assert.Equal(written, (int)reader.Consumed);
        }

        [Fact]
        public void VariableByteIntegerRejectsMoreThanFourBytes()
        {
            byte[] encoded = [0x80, 0x80, 0x80, 0x80, 0x01];
            SequenceReader<byte> reader = new(new ReadOnlySequence<byte>(encoded));

            Assert.False(
                MqttZeroWireFormat.TryReadVariableByteInteger(
                    ref reader,
                    out int value));
            Assert.Equal(0, value);
        }

        [Fact]
        public void VariableByteIntegerRejectsTruncatedInput()
        {
            byte[] encoded = [0x80];
            SequenceReader<byte> reader = new(new ReadOnlySequence<byte>(encoded));

            Assert.False(
                MqttZeroWireFormat.TryReadVariableByteInteger(
                    ref reader,
                    out _));
        }

        [Fact]
        public void UInt16ReadsBigEndian()
        {
            byte[] encoded = [0x12, 0x34];
            SequenceReader<byte> reader = new(new ReadOnlySequence<byte>(encoded));

            Assert.True(MqttZeroWireFormat.TryReadUInt16(ref reader, out ushort value));
            Assert.Equal(0x1234, value);
            Assert.Equal(2, (int)reader.Consumed);
        }

        [Fact]
        public void UInt16RejectsSingleByte()
        {
            byte[] encoded = [0x12];
            SequenceReader<byte> reader = new(new ReadOnlySequence<byte>(encoded));

            Assert.False(MqttZeroWireFormat.TryReadUInt16(ref reader, out _));
        }

        [Fact]
        public void AdvanceAndSkipBinaryGuardTheReader()
        {
            byte[] encoded = [0x00, 0x02, 0xAA, 0xBB];
            SequenceReader<byte> reader = new(new ReadOnlySequence<byte>(encoded));

            Assert.False(MqttZeroWireFormat.TryAdvance(ref reader, 5));
            Assert.True(MqttZeroWireFormat.TrySkipBinary(ref reader));
            Assert.Equal(4, (int)reader.Consumed);
            Assert.False(MqttZeroWireFormat.TrySkipBinary(ref reader));
        }
    }
}
