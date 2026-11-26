using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Tests.Protocol
{
    public sealed class Mqtt5PropertyLayoutTests
    {
        public static TheoryData<byte, int> PropertyLengthCases
        {
            get
            {
                return new TheoryData<byte, int>
                {
                    { Mqtt5PropertyId.PayloadFormatIndicator, 2 },
                    { Mqtt5PropertyId.MessageExpiryInterval, 5 },
                    { Mqtt5PropertyId.ContentType, 5 },
                    { Mqtt5PropertyId.ResponseTopic, 5 },
                    { Mqtt5PropertyId.CorrelationData, 5 },
                    { Mqtt5PropertyId.SubscriptionIdentifier, 2 },
                    { Mqtt5PropertyId.SessionExpiryInterval, 5 },
                    { Mqtt5PropertyId.AssignedClientIdentifier, 5 },
                    { Mqtt5PropertyId.ServerKeepAlive, 3 },
                    { Mqtt5PropertyId.AuthenticationMethod, 5 },
                    { Mqtt5PropertyId.AuthenticationData, 5 },
                    { Mqtt5PropertyId.RequestProblemInformation, 2 },
                    { Mqtt5PropertyId.WillDelayInterval, 5 },
                    { Mqtt5PropertyId.RequestResponseInformation, 2 },
                    { Mqtt5PropertyId.ResponseInformation, 5 },
                    { Mqtt5PropertyId.ServerReference, 5 },
                    { Mqtt5PropertyId.ReasonString, 5 },
                    { Mqtt5PropertyId.ReceiveMaximum, 3 },
                    { Mqtt5PropertyId.TopicAliasMaximum, 3 },
                    { Mqtt5PropertyId.TopicAlias, 3 },
                    { Mqtt5PropertyId.MaximumQos, 2 },
                    { Mqtt5PropertyId.RetainAvailable, 2 },
                    { Mqtt5PropertyId.UserProperty, 7 },
                    { Mqtt5PropertyId.MaximumPacketSize, 5 },
                    { Mqtt5PropertyId.WildcardSubscriptionAvailable, 2 },
                    { Mqtt5PropertyId.SubscriptionIdentifierAvailable, 2 },
                    { Mqtt5PropertyId.SharedSubscriptionAvailable, 2 }
                };
            }
        }

        [Theory]
        [MemberData(nameof(PropertyLengthCases))]
        public void SpanLengthMatchesReaderSkipForEveryProperty(
            byte identifier,
            int expectedLength)
        {
            byte[] encoded = EncodeProperty(identifier, expectedLength);

            Assert.True(
                Mqtt5PropertyLayout.TryGetPropertyLength(encoded, out int length));
            Assert.Equal(expectedLength, length);

            byte[] value = encoded[1..];
            SequenceReader<byte> reader = new(new ReadOnlySequence<byte>(value));
            Assert.True(Mqtt5PropertyLayout.TrySkip(identifier, ref reader));
            Assert.Equal(value.Length, (int)reader.Consumed);
        }

        [Theory]
        [MemberData(nameof(PropertyLengthCases))]
        public void TruncatedPropertyIsRejected(byte identifier, int expectedLength)
        {
            byte[] encoded = EncodeProperty(identifier, expectedLength);
            ReadOnlySpan<byte> truncated = encoded.AsSpan(0, expectedLength - 1);

            Assert.False(Mqtt5PropertyLayout.TryGetPropertyLength(truncated, out _));

            byte[] readerInput = truncated[1..].ToArray();
            SequenceReader<byte> reader = new(new ReadOnlySequence<byte>(readerInput));
            Assert.False(Mqtt5PropertyLayout.TrySkip(identifier, ref reader));
        }

        [Theory]
        [InlineData((byte)0x00)]
        [InlineData((byte)0x04)]
        [InlineData((byte)0x7F)]
        public void UnknownPropertyIsRejected(byte identifier)
        {
            byte[] encoded = [identifier, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

            Assert.False(Mqtt5PropertyLayout.TryGetPropertyLength(encoded, out _));

            SequenceReader<byte> reader = new(
                new ReadOnlySequence<byte>(encoded[1..]));
            Assert.False(Mqtt5PropertyLayout.TrySkip(identifier, ref reader));
        }

        [Fact]
        public void EmptyPropertiesAreRejected()
        {
            Assert.False(Mqtt5PropertyLayout.TryGetPropertyLength([], out int length));
            Assert.Equal(0, length);
        }

        [Fact]
        public void BinaryPairRequiresBothValues()
        {
            byte[] encoded =
            [
                Mqtt5PropertyId.UserProperty,
                0x00,
                0x01,
                (byte)'a',
                0x00,
                0x01
            ];

            Assert.False(Mqtt5PropertyLayout.TryGetPropertyLength(encoded, out _));
        }

        private static byte[] EncodeProperty(byte identifier, int expectedLength)
        {
            byte[] encoded = new byte[expectedLength];
            encoded[0] = identifier;
            switch (Mqtt5PropertyLayout.GetShape(identifier))
            {
                case Mqtt5PropertyShape.Byte:
                case Mqtt5PropertyShape.TwoByte:
                case Mqtt5PropertyShape.FourByte:
                    break;
                case Mqtt5PropertyShape.VariableByteInteger:
                    encoded[1] = 0x05;
                    break;
                case Mqtt5PropertyShape.Binary:
                    encoded[1] = 0x00;
                    encoded[2] = 0x02;
                    break;
                case Mqtt5PropertyShape.BinaryPair:
                    encoded[1] = 0x00;
                    encoded[2] = 0x01;
                    encoded[4] = 0x00;
                    encoded[5] = 0x01;
                    break;
                default:
                    throw new InvalidOperationException("Unknown property shape.");
            }

            return encoded;
        }
    }
}
