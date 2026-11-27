namespace MqttZero.Server.Broker;

/// <summary>
/// 入站缓冲区解析后的下一步动作。
/// </summary>
internal enum MqttZeroInboundBufferAction
{
    NotHandled,
    Handled,
    Stop
}
