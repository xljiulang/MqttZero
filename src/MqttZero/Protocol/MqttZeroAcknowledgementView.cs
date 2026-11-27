namespace MqttZero.Protocol;

/// <summary>
/// 表示 struct 的类型。
/// </summary>
internal readonly record struct MqttZeroAcknowledgementView(
    MqttPacketType PacketType,
    ushort PacketIdentifier,
    MqttReasonCode ReasonCode);
