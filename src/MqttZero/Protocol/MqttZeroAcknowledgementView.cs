namespace MqttZero.Protocol;

internal readonly record struct MqttZeroAcknowledgementView(
    MqttPacketType PacketType,
    ushort PacketIdentifier,
    MqttReasonCode ReasonCode);
