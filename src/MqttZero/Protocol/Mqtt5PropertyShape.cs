namespace MqttZero.Protocol;

/// <summary>
/// 单个属性值在线上占用的形状。
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
