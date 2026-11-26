using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Tests.Protocol
{
    public sealed class MqttZeroPacketWriterTests
    {
        [Fact]
        public void WriterSupportsPrimitivesBytesAndReset()
        {
            MqttZeroPacketWriter writer = new();

            writer.WriteByte(0x01);
            writer.WriteUInt16(0x0203);
            writer.WriteUInt32(0x04050607);
            writer.WriteBytes("ab"u8);
            writer.WriteVariableByteInteger(128);

            Assert.Equal(
                new byte[]
                {
                    0x01,
                    0x02,
                    0x03,
                    0x04,
                    0x05,
                    0x06,
                    0x07,
                    (byte)'a',
                    (byte)'b',
                    0x80,
                    0x01
                },
                writer.WrittenMemory.ToArray());

            writer.Reset(2);

            Assert.Equal(2, writer.WrittenCount);
        }

        [Fact]
        public void SeekAndAdvanceControlLogicalLength()
        {
            MqttZeroPacketWriter writer = new();
            writer.WriteBytes("abcdef"u8);

            writer.Seek(2);
            writer.Advance(2);

            Assert.Equal(6, writer.WrittenCount);
        }

        [Fact]
        public void WriterGrowsForLargeMemoryRequest()
        {
            MqttZeroPacketWriter writer = new();
            Memory<byte> memory = writer.GetMemory(16 * 1024);
            memory.Span[0] = 42;
            writer.Advance(1);

            Assert.Equal(42, writer.WrittenMemory.Span[0]);
        }
    }
}
