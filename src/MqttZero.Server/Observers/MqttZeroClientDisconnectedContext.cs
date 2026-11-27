using MqttZero.Server.Clients;
using MqttZero.Protocol;

namespace MqttZero.Server.Observers;

/// <summary>
/// 客户端连接断开事件。
/// </summary>
public sealed class MqttZeroClientDisconnectedContext
{
    /// <summary>获取客户端信息。</summary>
    public required MqttZeroClientConnectionInfo Client { get; init; }
    /// <summary>获取断开类型。</summary>
    public MqttZeroDisconnectType DisconnectType { get; init; }
    /// <summary>获取 MQTT 原因码。</summary>
    public MqttReasonCode? ReasonCode { get; init; }
}
