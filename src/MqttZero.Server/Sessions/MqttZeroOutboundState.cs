namespace MqttZero.Server.Sessions;

/// <summary>
/// 出站消息的 QoS 状态。
/// </summary>
internal enum MqttZeroOutboundState
{
    AwaitPubAck,
    AwaitPubRec,
    AwaitPubComp
}
