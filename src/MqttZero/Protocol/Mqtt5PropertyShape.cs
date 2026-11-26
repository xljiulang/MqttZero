namespace MqttZero.Protocol;

/// <summary>
/// Describes the wire shape of one MQTT 5 property value.
/// </summary>
internal enum Mqtt5PropertyShape
{
    Unknown,
    Byte,
    TwoByte,
    FourByte,
    Binary,
    BinaryPair,
    VariableByteInteger
}
