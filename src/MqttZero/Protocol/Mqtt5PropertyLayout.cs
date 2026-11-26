using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// Provides the single MQTT 5 property layout table used by readers, writers
/// and property transformers.
/// </summary>
internal static class Mqtt5PropertyLayout
{
    /// <summary>
    /// Gets the wire shape of one MQTT 5 property identifier.
    /// </summary>
    public static Mqtt5PropertyShape GetShape(byte identifier)
    {
        return identifier switch
        {
            Mqtt5PropertyId.PayloadFormatIndicator or
                Mqtt5PropertyId.RequestProblemInformation or
                Mqtt5PropertyId.RequestResponseInformation or
                Mqtt5PropertyId.MaximumQos or
                Mqtt5PropertyId.RetainAvailable or
                Mqtt5PropertyId.WildcardSubscriptionAvailable or
                Mqtt5PropertyId.SubscriptionIdentifierAvailable or
                Mqtt5PropertyId.SharedSubscriptionAvailable =>
                Mqtt5PropertyShape.Byte,
            Mqtt5PropertyId.ServerKeepAlive or
                Mqtt5PropertyId.ReceiveMaximum or
                Mqtt5PropertyId.TopicAliasMaximum or
                Mqtt5PropertyId.TopicAlias =>
                Mqtt5PropertyShape.TwoByte,
            Mqtt5PropertyId.MessageExpiryInterval or
                Mqtt5PropertyId.SessionExpiryInterval or
                Mqtt5PropertyId.WillDelayInterval or
                Mqtt5PropertyId.MaximumPacketSize =>
                Mqtt5PropertyShape.FourByte,
            Mqtt5PropertyId.SubscriptionIdentifier =>
                Mqtt5PropertyShape.VariableByteInteger,
            Mqtt5PropertyId.CorrelationData or
                Mqtt5PropertyId.AuthenticationData or
                Mqtt5PropertyId.ContentType or
                Mqtt5PropertyId.ResponseTopic or
                Mqtt5PropertyId.AssignedClientIdentifier or
                Mqtt5PropertyId.AuthenticationMethod or
                Mqtt5PropertyId.ResponseInformation or
                Mqtt5PropertyId.ServerReference or
                Mqtt5PropertyId.ReasonString =>
                Mqtt5PropertyShape.Binary,
            Mqtt5PropertyId.UserProperty =>
                Mqtt5PropertyShape.BinaryPair,
            _ => Mqtt5PropertyShape.Unknown
        };
    }

    /// <summary>
    /// Skips one encoded property value from a sequence reader.
    /// </summary>
    /// <param name="identifier">The property identifier.</param>
    /// <param name="reader">The property body reader.</param>
    /// <returns><see langword="true"/> when the value was skipped.</returns>
    public static bool TrySkip(byte identifier, ref SequenceReader<byte> reader)
    {
        switch (GetShape(identifier))
        {
            case Mqtt5PropertyShape.Byte:
                return MqttZeroWireFormat.TryAdvance(ref reader, 1);
            case Mqtt5PropertyShape.TwoByte:
                return MqttZeroWireFormat.TryAdvance(ref reader, 2);
            case Mqtt5PropertyShape.FourByte:
                return MqttZeroWireFormat.TryAdvance(ref reader, 4);
            case Mqtt5PropertyShape.VariableByteInteger:
                return MqttZeroWireFormat.TryReadVariableByteInteger(
                    ref reader,
                    out _);
            case Mqtt5PropertyShape.Binary:
                return MqttZeroWireFormat.TrySkipBinary(ref reader);
            case Mqtt5PropertyShape.BinaryPair:
                return MqttZeroWireFormat.TrySkipBinary(ref reader) &&
                    MqttZeroWireFormat.TrySkipBinary(ref reader);
            default:
                return false;
        }
    }

    /// <summary>
    /// Gets the encoded length of the first property, including its identifier.
    /// </summary>
    /// <param name="properties">The encoded properties to inspect.</param>
    /// <param name="length">The encoded property length when successful.</param>
    /// <returns><see langword="true"/> when one complete property was measured.</returns>
    public static bool TryGetPropertyLength(
        ReadOnlySpan<byte> properties,
        out int length)
    {
        length = 0;
        if (properties.IsEmpty)
        {
            return false;
        }

        Mqtt5PropertyShape shape = GetShape(properties[0]);
        switch (shape)
        {
            case Mqtt5PropertyShape.Unknown:
                return false;
            case Mqtt5PropertyShape.VariableByteInteger:
                return TryGetVariableByteIntegerLength(properties, out length);
            case Mqtt5PropertyShape.Binary:
                return TryGetLengthPrefixedEnd(properties, 1, out length);
            case Mqtt5PropertyShape.BinaryPair:
                return TryGetLengthPrefixedEnd(properties, 1, out length) &&
                    TryGetLengthPrefixedEnd(properties, length, out length);
            case Mqtt5PropertyShape.Byte:
                length = 2;
                break;
            case Mqtt5PropertyShape.TwoByte:
                length = 3;
                break;
            default:
                length = 5;
                break;
        }

        return length <= properties.Length;
    }

    private static bool TryGetVariableByteIntegerLength(
        ReadOnlySpan<byte> properties,
        out int length)
    {
        length = 0;
        int limit = Math.Min(properties.Length, 5);
        for (int index = 1; index < limit; index++)
        {
            if ((properties[index] & 0x80) == 0)
            {
                length = index + 1;
                return true;
            }
        }

        return false;
    }

    private static bool TryGetLengthPrefixedEnd(
        ReadOnlySpan<byte> properties,
        int offset,
        out int end)
    {
        end = 0;
        if (offset + 2 > properties.Length)
        {
            return false;
        }

        int payloadLength = (properties[offset] << 8) | properties[offset + 1];
        end = offset + 2 + payloadLength;
        return end <= properties.Length;
    }
}
